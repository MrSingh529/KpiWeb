using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.RegularExpressions;
using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.Data.Sqlite;
using System.Security.Cryptography;
using System.Text.Json;


var builder = WebApplication.CreateBuilder(args);
builder.Services.AddRouting();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddDirectoryBrowser();

var app = builder.Build();
app.UseDefaultFiles(new DefaultFilesOptions
{
    DefaultFileNames = new List<string> { "login.html" }
});
app.UseStaticFiles();

const string DbPath = "kpi.db";
const string TableName = "KpiData";

var validRegions = new HashSet<string> { "East", "West", "North", "South" };
var circlesByRegion = new Dictionary<string, HashSet<string>>
{
    ["North"] = new HashSet<string> { "DL","UPW","UPE" },
    ["East"]  = new HashSet<string> { "OD","WB","BH","NESA","ASM" },
    ["West"]  = new HashSet<string> { "MPCG","MH","MUM","RAJ","GJ" },
    ["South"] = new HashSet<string> { "TN","APTL","KL","KK" }
};
var validWorkDoneBy = new HashSet<string> { "Partner", "RVS" };

app.MapPost("/api/process-csv", async (HttpRequest req) =>
{
    if (!req.HasFormContentType)
        return Results.BadRequest(new { ok = false, message = "Invalid form content type." });

    var form = await req.ReadFormAsync();
    var file = form.Files["file"];
    var preview = form["preview"].ToString().Trim().ToLower() == "true";

    if (file == null || file.Length == 0)
        return Results.BadRequest(new { ok = false, message = "CSV file is required." });

    // NTP IST
    DateTime currentIst;
    try
    {
        currentIst = GetNetworkTimeIst();
    }
    catch
    {
        var ist = TimeZoneInfo.FindSystemTimeZoneById("India Standard Time");
        currentIst = TimeZoneInfo.ConvertTime(DateTime.UtcNow, ist);
    }

    DateTime uploadTimeIst;
    try
    {
        uploadTimeIst = GetNetworkTimeIst();
    }
    catch
    {
        uploadTimeIst = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow,
            TimeZoneInfo.FindSystemTimeZoneById("India Standard Time"));
    }
    var uploadTimestampIso = uploadTimeIst.ToString("o"); // ISO 8601

    var errors = new List<string>();
    var inserted = 0;
    List<string> csvHeaders;
    List<string> dbHeaders;

    var config = new CsvConfiguration(CultureInfo.InvariantCulture)
    {
        HasHeaderRecord = true,
        IgnoreBlankLines = true,
        TrimOptions = TrimOptions.Trim,
        BadDataFound = null,
        MissingFieldFound = null
    };

    // Read CSV from upload stream
    using var stream = file.OpenReadStream();
    using var reader = new StreamReader(stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, leaveOpen: true);
    using var csv = new CsvReader(reader, config);

    if (!csv.Read() || !csv.ReadHeader())
        return Results.BadRequest(new { ok = false, message = "CSV header missing or invalid." });

    csvHeaders = csv.HeaderRecord.ToList();

    // Build DB headers
    dbHeaders = new List<string> { "Business Category", "Category" };
    dbHeaders.AddRange(csvHeaders);
    dbHeaders.AddRange(new[]
    {
        "Workdone Week","Booking Month",
        "Customer Billing Status","Customer Bill Qty","Customer Billed Amount","Customer Billing Month",
        "Partner Bill no","PartnerWCC","Partner WCC mail Date","Partner Billed Qty","Partner Billed Rate","Partner Billed Amount"
    });

    if (!dbHeaders.Contains("Uploaded At"))
        dbHeaders.Add("Uploaded At");

    // Create/Reset table
    //EnsureDatabaseAndTable(dbHeaders);

    // ---- FETCH ALL DB ROW KEYS FOR DUPLICATE CHECK ----
    var dbRowKeys = new HashSet<string>();
    using (var checkConn = new SqliteConnection($"Data Source={DbPath}"))
    {
        await checkConn.OpenAsync();
        using (var cmd = checkConn.CreateCommand())
        {
            cmd.CommandText = $@"
                SELECT [Region],[Circle],[Project Name (As per PMS)],[Project Name (As per Project Team)],
                       [Customer],[Customer Qty],[Site ID],[Site ID as per Suffex],[SLI Code],[Customer SLI],
                       [Customer Rate],[Customer Amount],[Date]
                FROM {TableName}";
            using (var rdr = await cmd.ExecuteReaderAsync())
            {
                while (await rdr.ReadAsync())
                {
                    string dbKey = string.Join("||", new[]
                    {
                        rdr["Region"]?.ToString() ?? "",
                        rdr["Circle"]?.ToString() ?? "",
                        rdr["Project Name (As per PMS)"]?.ToString() ?? "",
                        rdr["Project Name (As per Project Team)"]?.ToString() ?? "",
                        rdr["Customer"]?.ToString() ?? "",
                        rdr["Customer Qty"]?.ToString() ?? "",
                        rdr["Site ID"]?.ToString() ?? "",
                        rdr["Site ID as per Suffex"]?.ToString() ?? "",
                        rdr["SLI Code"]?.ToString() ?? "",
                        rdr["Customer SLI"]?.ToString() ?? "",
                        rdr["Customer Rate"]?.ToString() ?? "",
                        rdr["Customer Amount"]?.ToString() ?? "",
                        rdr["Date"]?.ToString() ?? ""
                    });
                    dbRowKeys.Add(dbKey);
                }
            }
        }
    }

    // ---- Process rows from CSV ----
    var uniqueKeys = new HashSet<string>();
    var previews = new List<Dictionary<string, string>>();

    using var conn = new SqliteConnection($"Data Source={DbPath}");
    await conn.OpenAsync();
    using var tx = conn.BeginTransaction();

    int rowNumber = 1;
    while (await csv.ReadAsync())
    {
        var dict = dbHeaders.ToDictionary(
            h => h,
            h =>
            {
                if (h == "Business Category") return "Telecom Project";
                if (h == "Category") return "Onsite Projects";
                if (h == "Workdone Week" || h == "Booking Month" ||
                    h.StartsWith("Customer Billing") || h.StartsWith("Partner Bill")) return "";
                if (csvHeaders.Contains(h)) return (csv.GetField(h) ?? "").Trim();
                return "";
            }
        );

        dict["Uploaded At"] = uploadTimestampIso;

        var rowErrs = new List<string>();
        string Get(string key) => dict.TryGetValue(key, out var v) ? v : "";

        // (all mandatory/validation checks as before...)

        string[] mandatoryFields = {
            "Region","Circle","Project Name (As per PMS)","Project Name (As per Project Team)","Customer",
            "Customer Qty","Site ID","SLI Code","Customer SLI","Customer Rate","Customer Amount",
            "Date","Work Done by","Partner Name","Partner SLI","Partner Qty","Partner Rate","Partner Amount"
        };
        foreach (var m in mandatoryFields)
            if (string.IsNullOrWhiteSpace(Get(m))) rowErrs.Add($"Mandatory '{m}' missing.");

        var region = Get("Region");
        var circle = Get("Circle");
        if (!validRegions.Contains(region))
            rowErrs.Add("Region invalid.");
        else if (!circlesByRegion[region].Contains(circle))
            rowErrs.Add($"Circle '{circle}' invalid for region '{region}'.");

        var workDoneBy = Get("Work Done by");
        if (!validWorkDoneBy.Contains(workDoneBy))
            rowErrs.Add("Work Done by invalid.");

        var partnerName = Get("Partner Name");
        var partnerSLI = Get("Partner SLI");
        if (workDoneBy == "Partner")
        {
            if (partnerName.Equals("NA", StringComparison.OrdinalIgnoreCase))
                rowErrs.Add("Partner Name cannot be NA for Partner.");
            if (partnerSLI.Equals("NA", StringComparison.OrdinalIgnoreCase))
                rowErrs.Add("Partner SLI cannot be NA for Partner.");
        }
        else if (workDoneBy == "RVS")
        {
            if (!partnerName.Equals("NA", StringComparison.OrdinalIgnoreCase))
                rowErrs.Add("Partner Name must be NA for RVS.");
            if (!partnerSLI.Equals("NA", StringComparison.OrdinalIgnoreCase))
                rowErrs.Add("Partner SLI must be NA for RVS.");
        }

        if (!int.TryParse(Get("Customer Qty"), out int custQty))
            rowErrs.Add("Customer Qty must be digits.");
        if (!decimal.TryParse(Get("Customer Rate"), out decimal custRate))
            rowErrs.Add("Customer Rate invalid.");
        if (!decimal.TryParse(Get("Customer Amount"), out decimal custAmount))
            rowErrs.Add("Customer Amount invalid.");
        else if (Math.Round(custQty * custRate, 2) != Math.Round(custAmount, 2))
            rowErrs.Add("Customer Amount mismatch with Qty*Rate.");

        if (!decimal.TryParse(Get("Partner Qty"), out decimal partnerQty))
            rowErrs.Add("Partner Qty invalid.");
        if (!decimal.TryParse(Get("Partner Rate"), out decimal partnerRate))
            rowErrs.Add("Partner Rate invalid.");
        if (!decimal.TryParse(Get("Partner Amount"), out decimal partnerAmount))
            rowErrs.Add("Partner Amount invalid.");
        else
        {
            if (workDoneBy == "Partner" && Math.Round(partnerQty * partnerRate, 2) != Math.Round(partnerAmount, 2))
                rowErrs.Add("Partner Amount mismatch with Qty*Rate.");
            if (workDoneBy == "RVS" && (partnerQty != 0 || partnerRate != 0 || partnerAmount != 0))
                rowErrs.Add("Partner Qty/Rate/Amount must be 0 for RVS.");
        }

        if (!Regex.IsMatch(Get("Site ID"), @"^[\w\-@#]+$"))
            rowErrs.Add("Site ID invalid format.");

        string dateStr = Get("Date").Trim();

        if (!DateTime.TryParseExact(dateStr, "dd-MMM-yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out var dateVal))
        {
            rowErrs.Add("Date format invalid, must be DD-MMM-YYYY.");
        }
        else if (dateVal.Date > DateTime.Now.Date)
        {
            rowErrs.Add("Date cannot be in the future.");
        }

        else
        {
            (string workWeek, string bookingMonth) = GetBusinessWorkdoneWeekAndBookingMonth(dateVal);
            dict["Workdone Week"] = workWeek;
            dict["Booking Month"] = bookingMonth;
        }

        // Build unique key for the row
        string uniqueKey = string.Join("||", new[]
        {
            Get("Region"), Get("Circle"), Get("Project Name (As per PMS)"), Get("Project Name (As per Project Team)"),
            Get("Customer"), Get("Customer Qty"), Get("Site ID"), Get("Site ID as per Suffex"), Get("SLI Code"),
            Get("Customer SLI"), Get("Customer Rate"), Get("Customer Amount"), Get("Date")
        });

        if (!uniqueKeys.Add(uniqueKey))
            rowErrs.Add("Duplicate row detected based on uniqueness key.");
        if (dbRowKeys.Contains(uniqueKey))
            rowErrs.Add("Duplicate row already exists in database.");

        if (rowErrs.Count > 0)
        {
            errors.Add($"Row {rowNumber}: {string.Join("; ", rowErrs)}");
        }
        else
        {
            using var cmd = conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = $@"
                INSERT INTO {TableName} ({string.Join(",", dbHeaders.Select(h => $"[{h}]"))})
                VALUES ({string.Join(",", dbHeaders.Select(h => "@" + Regex.Replace(h, @"[^\w]", "_")))})";
            foreach (var h in dbHeaders)
            {
                var paramName = "@" + Regex.Replace(h, @"[^\w]", "_");
                cmd.Parameters.AddWithValue(paramName, dict[h] ?? "");
            }
            inserted += await cmd.ExecuteNonQueryAsync();
        }

        if (preview)
        {
            var previewRow = new Dictionary<string, string>();
            foreach (var h in csvHeaders)
                previewRow[h] = dict[h];
            previewRow["_errors"] = string.Join("; ", rowErrs);
            previews.Add(previewRow);
        }
        rowNumber++;
    }

    if (errors.Count > 0)
    {
        await tx.RollbackAsync();
        return Results.Ok(new
        {
            ok = false,
            preview = preview ? previews : null,
            errors,
            message = "No rows were inserted due to errors."
        });
    }
    else
    {
        await tx.CommitAsync();
        return Results.Ok(new
        {
            ok = true,
            preview = preview ? previews : null,
            inserted,
            message = $"Inserted {inserted} valid rows into the database."
        });
    }
});

app.MapPost("/api/search-db", async (HttpRequest req) =>
{
    var form = await req.ReadFromJsonAsync<Dictionary<string, string>>();
    var filters = form ?? new();

    // Check if at least one filter has a non-empty value
    if (!filters.Any(kv => !string.IsNullOrWhiteSpace(kv.Value)))
    {
        return Results.BadRequest(new { ok = false, message = "Please provide at least one search parameter." });
    }

    using var conn = new SqliteConnection($"Data Source={DbPath}");
    await conn.OpenAsync();

    var where = new List<string>();
    var parameters = new Dictionary<string, object>();
    foreach (var kv in filters)
    {
        if (!string.IsNullOrWhiteSpace(kv.Value))
        {
            var safeParamName = kv.Key.Replace(" ", "_");
            where.Add($"[{kv.Key}] LIKE @{safeParamName}");
            parameters.Add($"@{safeParamName}", $"%{kv.Value}%");
        }
    }
    var whereClause = where.Count > 0 ? ("WHERE " + string.Join(" AND ", where)) : "";

    var sql = $"SELECT * FROM {TableName} {whereClause}";
    using var cmd = new SqliteCommand(sql, conn);

    foreach (var kv in parameters)
        cmd.Parameters.AddWithValue(kv.Key, kv.Value);

    var reader = await cmd.ExecuteReaderAsync();

    var rows = new List<Dictionary<string, string>>();
    var colNames = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToList();
    while (await reader.ReadAsync())
    {
        var row = new Dictionary<string, string>();
        foreach (var c in colNames)
            row[c] = reader[c]?.ToString() ?? "";
        rows.Add(row);
    }
    return Results.Ok(new { ok = true, columns = colNames, rows });
});



app.MapPost("/api/update-row", async (HttpRequest req) =>
{
    var data = await req.ReadFromJsonAsync<Dictionary<string, string>>();
    if (data == null || !data.ContainsKey("Id"))
        return Results.BadRequest(new { ok = false, message = "Id is required." });

    var idString = data["Id"];
    data.Remove("Id");

    // Prevent update of system fields
    if (data.ContainsKey("Business Category"))
        data.Remove("Business Category");
    if (data.ContainsKey("Category"))
        data.Remove("Category");

    if (!int.TryParse(idString, out int id))
        return Results.BadRequest(new { ok = false, message = "Invalid Id format." });

    if (data.Count == 0)
        return Results.BadRequest(new { ok = false, message = "No fields to update." });

    using var conn = new SqliteConnection($"Data Source={DbPath}");
    await conn.OpenAsync();

    // Check record existence
    using var checkCmd = conn.CreateCommand();
    checkCmd.CommandText = $"SELECT COUNT(1) FROM {TableName} WHERE Id = @Id";
    checkCmd.Parameters.AddWithValue("@Id", id);
    if ((long)await checkCmd.ExecuteScalarAsync() == 0)
        return Results.BadRequest(new { ok = false, message = "Record not found." });

    // Prepare existing keys for validation
    var existingKeys = new HashSet<string>();
    using var keyCmd = conn.CreateCommand();
    keyCmd.CommandText = $@"SELECT 
        TRIM([Region]), TRIM([Circle]), [Project Name (As per PMS)], [Project Name (As per Project Team)],
        [Customer], [Customer Qty], [Site ID], [Site ID as per Suffex], [SLI Code], [Customer SLI],
        [Customer Rate], [Customer Amount], [Date]
        FROM {TableName} WHERE Id != @Id";
    keyCmd.Parameters.AddWithValue("@Id", id);
    using var reader = await keyCmd.ExecuteReaderAsync();
    while (await reader.ReadAsync())
    {
        var key = string.Join("||", new[]
        {
            reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetString(3),
            reader.GetString(4), reader.GetString(5), reader.GetString(6), reader.GetString(7),
            reader.GetString(8), reader.GetString(9), reader.GetString(10), reader.GetString(11),
            reader.GetString(12)
        });
        existingKeys.Add(key);
    }

    // Run validation
    var validationErrors = ValidateKpiRow(data, existingKeys, idString);
    if (validationErrors.Count > 0)
    {
        // Log each validation error on console for debugging
        foreach (var err in validationErrors)
            Console.WriteLine("Validation error: " + err);

        // Return validation errors in response
        return Results.BadRequest(new {
            ok = false,
            message = "Validation failed.",
            errors = validationErrors
        });
    }

    // Proceed to perform update if valid
    var setClauses = data.Keys.Select(k => $"[{k}] = @{k.Replace(" ", "_")}");
    var sql = $"UPDATE {TableName} SET {string.Join(", ", setClauses)} WHERE Id = @Id";
    using var updateCmd = conn.CreateCommand();
    updateCmd.CommandText = sql;
    updateCmd.Parameters.AddWithValue("@Id", id);
    foreach (var kvp in data)
    {
        var paramValue = string.IsNullOrWhiteSpace(kvp.Value) ? DBNull.Value : (object)kvp.Value;
        updateCmd.Parameters.AddWithValue("@" + kvp.Key.Replace(" ", "_"), paramValue);
    }

    int affected = await updateCmd.ExecuteNonQueryAsync();
    if (affected == 0)
        return Results.BadRequest(new { ok = false, message = "No rows were updated." });

    return Results.Ok(new { ok = true, message = "Record updated successfully." });
});









// DELETE endpoint (no changes needed except remove backslashes)
app.MapPost("/api/delete-row", async (HttpRequest req) =>
{
    var data = await req.ReadFromJsonAsync<Dictionary<string, string>>();
    if (data == null || !data.ContainsKey("Id"))
        return Results.BadRequest(new { ok = false });

    var id = data["Id"];
    using var conn = new SqliteConnection($"Data Source={DbPath}");
    await conn.OpenAsync();

    var sql = $"DELETE FROM {TableName} WHERE \"Id\" = @Id";
    using var cmd = conn.CreateCommand();
    cmd.CommandText = sql;
    cmd.Parameters.AddWithValue("@Id", id);

    var affected = await cmd.ExecuteNonQueryAsync();
    return Results.Ok(new { ok = affected > 0 });
});

app.MapPost("/api/users/reset-password", async (HttpRequest req) =>
{
    var data = await req.ReadFromJsonAsync<Dictionary<string, string>>();
    if (data == null || !data.ContainsKey("Id"))
        return Results.BadRequest(new { ok = false, message = "User Id required" });

    string id = data["Id"];
    string newPassword = data.ContainsKey("Password") ? data["Password"] : "default123"; // fallback password

    string newPassHash = HashPassword(newPassword);

    using var conn = new SqliteConnection($"Data Source={DbPath}");
    await conn.OpenAsync();
    var cmd = conn.CreateCommand();
    cmd.CommandText = "UPDATE Users SET PasswordHash = @passHash WHERE Id = @id";
    cmd.Parameters.AddWithValue("@passHash", newPassHash);
    cmd.Parameters.AddWithValue("@id", id);

    int updated = await cmd.ExecuteNonQueryAsync();

    return updated > 0 ? Results.Ok(new { ok = true, message = "Password reset successfully", password = newPassword }) :
                         Results.BadRequest(new { ok = false, message = "Password reset failed or user not found" });
});


app.MapGet("/api/view-db", async () =>
{
    using var conn = new SqliteConnection($"Data Source={DbPath}");
    await conn.OpenAsync();
    using var cmd = conn.CreateCommand();
    cmd.CommandText = $"SELECT * FROM {TableName}";
    using var reader = await cmd.ExecuteReaderAsync();

    var rows = new List<Dictionary<string, string>>();
    var colNames = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToList();
    while (await reader.ReadAsync())
    {
        var row = new Dictionary<string, string>();
        foreach (var c in colNames)
            row[c] = reader[c]?.ToString() ?? "";
        rows.Add(row);
    }
    return Results.Ok(new { ok = true, columns = colNames, rows });
});

app.MapGet("/api/export", async (HttpContext ctx) =>
{
    var q = ctx.Request.Query;
    bool all = string.Equals(q["all"], "true", StringComparison.OrdinalIgnoreCase);
    bool debug = string.Equals(q["debug"], "true", StringComparison.OrdinalIgnoreCase);

    var week = q["week"].ToString();
    var month = q["month"].ToString();
    var weeksRaw = q["weeks"].ToString();
    var monthsRaw = q["months"].ToString();
    var yearsRaw = q["years"].ToString();

    if (!all && string.IsNullOrWhiteSpace(week) && string.IsNullOrWhiteSpace(weeksRaw)
        && string.IsNullOrWhiteSpace(month) && string.IsNullOrWhiteSpace(monthsRaw)
        && string.IsNullOrWhiteSpace(yearsRaw))
    {
        ctx.Response.StatusCode = 400;
        await ctx.Response.WriteAsJsonAsync(new { ok = false, message = "Provide filters: all=true OR week+month OR weeks OR months OR years." });
        return;
    }

    try
    {
        using var conn = new SqliteConnection($"Data Source={DbPath}");
        await conn.OpenAsync();

        var whereClauses = new List<string>();
        var parameters = new Dictionary<string, object>();
        // Legacy exact
        bool usingLegacyExact = !string.IsNullOrWhiteSpace(week) && !string.IsNullOrWhiteSpace(month)
                                && string.IsNullOrWhiteSpace(weeksRaw) && string.IsNullOrWhiteSpace(monthsRaw) && string.IsNullOrWhiteSpace(yearsRaw)
                                && !all;
        if (usingLegacyExact)
        {
            whereClauses.Add($"\"Workdone Week\" = @w_exact AND \"Booking Month\" = @m_exact");
            parameters.Add("@w_exact", week);
            parameters.Add("@m_exact", month);
        }
        else if (!all)
        {
            if (!string.IsNullOrWhiteSpace(weeksRaw))
            {
                var tokens = weeksRaw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                                     .Select(t => t.Trim())
                                     .Where(t => t.Length > 0)
                                     .Select(t =>
                                     {
                                         if (Regex.IsMatch(t, @"^\d+$")) return "W" + t;
                                         if (Regex.IsMatch(t, @"^W\d+$", RegexOptions.IgnoreCase)) return t.ToUpper();
                                         return t;
                                     })
                                     .Distinct()
                                     .ToArray();
                if (tokens.Length > 0)
                {
                    var parts = new List<string>();
                    for (int i = 0; i < tokens.Length; i++)
                    {
                        var key = $"@wk{i}";
                        parts.Add($"\"Workdone Week\" LIKE {key}");
                        parameters.Add(key, "%" + tokens[i]);
                    }
                    whereClauses.Add("(" + string.Join(" OR ", parts) + ")");
                }
            }

            if (!string.IsNullOrWhiteSpace(monthsRaw))
            {
                var tokens = monthsRaw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                                      .Select(t => t.Trim())
                                      .Where(t => t.Length > 0)
                                      .Distinct()
                                      .ToArray();
                var parts = new List<string>();
                for (int i = 0; i < tokens.Length; i++)
                {
                    var t = tokens[i];
                    string normalized = t;
                    var ym = Regex.Match(t, @"^(\d{4})-(\d{1,2})$");
                    if (ym.Success)
                    {
                        int mm = int.Parse(ym.Groups[2].Value);
                        var mon = new DateTime(int.Parse(ym.Groups[1].Value), mm, 1)
                                  .ToString("MMM", CultureInfo.InvariantCulture);
                        normalized = $"{mon}-{(int.Parse(ym.Groups[1].Value) % 100):D2}";
                    }
                    var key = $"@mo{i}";
                    parts.Add($"\"Booking Month\" LIKE {key}");
                    parameters.Add(key, "%" + normalized);
                }
                if (parts.Count > 0) whereClauses.Add("(" + string.Join(" OR ", parts) + ")");
            }

            if (!string.IsNullOrWhiteSpace(yearsRaw))
            {
                var tokens = yearsRaw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                                     .Select(t => t.Trim())
                                     .Where(t => t.Length > 0)
                                     .Distinct()
                                     .ToArray();
                var parts = new List<string>();
                for (int i = 0; i < tokens.Length; i++)
                {
                    var t = tokens[i];
                    string yy;
                    var m = Regex.Match(t, @"^\d{4}$");
                    if (m.Success) yy = (int.Parse(t) % 100).ToString("D2");
                    else yy = t;
                    var key = $"@yr{i}";
                    parts.Add($"\"Booking Month\" LIKE {key}");
                    parameters.Add(key, "%-" + yy);
                }
                if (parts.Count > 0) whereClauses.Add("(" + string.Join(" OR ", parts) + ")");
            }
        }

        var sql = new StringBuilder($"SELECT * FROM \"{TableName}\"");
        if (!all && whereClauses.Count > 0)
        {
            sql.Append(" WHERE ");
            sql.Append(string.Join(" AND ", whereClauses));
        }
        else if (!all && whereClauses.Count == 0)
        {
            ctx.Response.StatusCode = 400;
            await ctx.Response.WriteAsJsonAsync(new { ok = false, message = "No valid filters provided." });
            return;
        }

        using var cmd = new SqliteCommand(sql.ToString(), conn);
        foreach (var kv in parameters) cmd.Parameters.AddWithValue(kv.Key, kv.Value ?? "");
        using var reader = await cmd.ExecuteReaderAsync();

        // read all rows into memory (needed for debug response)
        var headers = new List<string>();
        for (int i = 0; i < reader.FieldCount; i++) headers.Add(reader.GetName(i));
        var rows = new List<string[]>();
        while (await reader.ReadAsync())
        {
            var fields = new string[reader.FieldCount];
            for (int i = 0; i < reader.FieldCount; i++)
            {
                fields[i] = reader.IsDBNull(i) ? "" : reader.GetValue(i)?.ToString();
            }
            rows.Add(fields);
        }

        if (debug)
        {
            var sample = rows.Take(10)
                             .Select(r => headers.Select((h, idx) => new { h, v = r[idx] })
                                                 .ToDictionary(x => x.h, x => x.v))
                             .ToList();
            await ctx.Response.WriteAsJsonAsync(new
            {
                ok = true,
                sql = sql.ToString(),
                parameters,
                rowCount = rows.Count,
                sample
            });
            return;
        }

        // build CSV from headers + rows
        string Quote(string s) => s == null ? "" : $"\"{s.Replace("\"", "\"\"")}\"";
        var sb = new StringBuilder();
        sb.AppendLine(string.Join(",", headers.Select(Quote)));
        foreach (var r in rows)
        {
            sb.AppendLine(string.Join(",", r.Select(Quote)));
        }

        var fileName = $"Export_{(all ? "ALL" : DateTime.UtcNow.ToString("yyyyMMdd_HHmmss"))}.csv";
        ctx.Response.Headers.ContentType = "text/csv; charset=utf-8";
        ctx.Response.Headers.ContentDisposition = $"attachment; filename=\"{fileName}\"";
        await ctx.Response.WriteAsync(sb.ToString());
    }
    catch (Exception ex)
    {
        ctx.Response.StatusCode = 500;
        await ctx.Response.WriteAsJsonAsync(new { ok = false, message = "Export failed.", error = ex.Message });
    }
});

app.MapPost("/api/login", async (HttpRequest req) =>
{
    var loginData = await req.ReadFromJsonAsync<Dictionary<string, string>>();
    if (loginData == null || !loginData.ContainsKey("username") || !loginData.ContainsKey("password"))
        return Results.BadRequest("Username and password required.");

    var username = loginData["username"];
    var password = loginData["password"];

    using var conn = new SqliteConnection($"Data Source={DbPath}");
    await conn.OpenAsync();

    using var cmd = conn.CreateCommand();
    cmd.CommandText = @"SELECT PasswordHash, IsAdmin FROM Users WHERE Username = @u LIMIT 1";
    cmd.Parameters.AddWithValue("@u", username);

    using var reader = await cmd.ExecuteReaderAsync();

    if (await reader.ReadAsync())
    {
        var storedHash = reader.GetString(0);
        var isAdmin = reader.GetInt32(1) == 1;

        if (VerifyPassword(password, storedHash))
        {
            // Optionally generate JWT or session token here
            return Results.Ok(new { ok = true, message = "Login successful", isAdmin });
        }
    }

    return Results.Unauthorized();
});

app.MapGet("/api/users", async () =>
{
    using var conn = new SqliteConnection($"Data Source={DbPath}");
    await conn.OpenAsync();

    using var cmd = conn.CreateCommand();
    cmd.CommandText = @"SELECT Id, Username, IsAdmin, CreatedAt FROM Users";

    var users = new List<Dictionary<string, object>>();

    using var reader = await cmd.ExecuteReaderAsync();
    while (await reader.ReadAsync())
    {
        users.Add(new Dictionary<string, object>
        {
            ["Id"] = reader.GetInt32(0),
            ["Username"] = reader.GetString(1),
            ["IsAdmin"] = reader.GetInt32(2) == 1,
            ["CreatedAt"] = reader.GetDateTime(3)
        });
    }

    return Results.Ok(new { ok = true, users });
});

app.MapPost("/api/users/update", async (HttpRequest req) =>
{
    var data = await req.ReadFromJsonAsync<Dictionary<string, string>>();
    if (data == null || !data.ContainsKey("Id")) return Results.BadRequest();

    int id = int.Parse(data["Id"]);
    string? username = data.ContainsKey("Username") ? data["Username"] : null;
    string? password = data.ContainsKey("Password") ? data["Password"] : null;
    bool? isAdmin = data.ContainsKey("IsAdmin") ? (data["IsAdmin"] == "true") : null;

    using var conn = new SqliteConnection($"Data Source={DbPath}");
    await conn.OpenAsync();

    var updates = new List<string>();
    var parameters = new Dictionary<string, object>();

    if (username != null) { updates.Add("[Username] = @username"); parameters.Add("@username", username); }
    if (password != null) { updates.Add("[PasswordHash] = @passwordHash"); parameters.Add("@passwordHash", HashPassword(password)); }
    if (isAdmin.HasValue) { updates.Add("[IsAdmin] = @isAdmin"); parameters.Add("@isAdmin", isAdmin.Value ? 1 : 0); }

    if (updates.Count == 0) return Results.BadRequest("No fields to update.");

    string setClause = string.Join(", ", updates);

    using var cmd = conn.CreateCommand();
    cmd.CommandText = $"UPDATE Users SET {setClause} WHERE Id = @id";
    cmd.Parameters.AddWithValue("@id", id);
    foreach (var p in parameters)
    {
        cmd.Parameters.AddWithValue(p.Key, p.Value);
    }

    int affected = await cmd.ExecuteNonQueryAsync();
    return Results.Ok(new { ok = affected > 0 });
});

app.MapPost("/api/users/delete", async (HttpRequest req) =>
{
    var data = await req.ReadFromJsonAsync<Dictionary<string, int>>();
    if (data == null || !data.ContainsKey("Id")) return Results.BadRequest();

    int id = data["Id"];

    using var conn = new SqliteConnection($"Data Source={DbPath}");
    await conn.OpenAsync();

    using var cmd = conn.CreateCommand();
    cmd.CommandText = "DELETE FROM Users WHERE Id = @id";
    cmd.Parameters.AddWithValue("@id", id);

    int affected = await cmd.ExecuteNonQueryAsync();
    return Results.Ok(new { ok = affected > 0 });
});



app.MapPost("/api/users/create", async (HttpRequest req) =>
{
    var userData = await req.ReadFromJsonAsync<Dictionary<string, object>>();
    if (userData == null || !userData.ContainsKey("username") || !userData.ContainsKey("password"))
        return Results.BadRequest("Username and password required.");

    string username = userData["username"].ToString();
    string password = userData["password"].ToString();

    bool isAdmin = false;
    if (userData.ContainsKey("isAdmin"))
    {
        if (userData["isAdmin"] is JsonElement je)
        {
            if (je.ValueKind == JsonValueKind.True) 
                isAdmin = true;
            else if (je.ValueKind == JsonValueKind.False) 
                isAdmin = false;
            else if (je.ValueKind == JsonValueKind.String) 
                isAdmin = je.GetString()?.ToLower() == "true";
        }
        else
        {
            // fallback for string or bool
            isAdmin = userData["isAdmin"].ToString().ToLower() == "true";
        }
    }

    var passwordHash = HashPassword(password);

    using var conn = new SqliteConnection($"Data Source={DbPath}");
    await conn.OpenAsync();

    using var cmd = conn.CreateCommand();
    cmd.CommandText = @"INSERT INTO Users (Username, PasswordHash, IsAdmin) VALUES (@u, @p, @a)";
    cmd.Parameters.AddWithValue("@u", username);
    cmd.Parameters.AddWithValue("@p", passwordHash);
    cmd.Parameters.AddWithValue("@a", isAdmin ? 1 : 0);

    try
    {
        int result = await cmd.ExecuteNonQueryAsync();
        return Results.Ok(new { ok = true, message = "User created." });
    }
    catch (SqliteException e) when (e.SqliteErrorCode == 19) // UNIQUE constraint failed
    {
        return Results.BadRequest(new { ok = false, message = "Username already exists." });
    }
});

app.MapPost("/api/user-search", async (HttpRequest req) =>
{
    var filters = await req.ReadFromJsonAsync<Dictionary<string, string>>();
    // Validate filters; check if at least one filter present
    if (filters == null || !filters.Any(kv => !string.IsNullOrWhiteSpace(kv.Value)))
        return Results.BadRequest(new { ok = false, message = "At least one search parameter required." });

    using var conn = new SqliteConnection($"Data Source={DbPath}");
    await conn.OpenAsync();

    var whereClauses = new List<string>();
    var parameters = new Dictionary<string, object>();

    if (filters.TryGetValue("Username", out var username) && !string.IsNullOrWhiteSpace(username))
    {
        whereClauses.Add("Username LIKE @username");
        parameters.Add("@username", $"%{username}%");
    }

    if (filters.TryGetValue("IsAdmin", out var isAdminStr) && !string.IsNullOrWhiteSpace(isAdminStr))
    {
        bool isAdmin = isAdminStr.ToLower() == "true";
        whereClauses.Add("IsAdmin = @isAdmin");
        parameters.Add("@isAdmin", isAdmin ? 1 : 0);
    }

    var whereSql = whereClauses.Any() ? "WHERE " + string.Join(" AND ", whereClauses) : "";

    var sql = $"SELECT Id, Username, IsAdmin, CreatedAt FROM Users {whereSql}";

    using var cmd = new SqliteCommand(sql, conn);

    foreach (var param in parameters)
        cmd.Parameters.AddWithValue(param.Key, param.Value);

    var reader = await cmd.ExecuteReaderAsync();
    var users = new List<Dictionary<string, string>>();

    while (await reader.ReadAsync())
    {
        users.Add(new Dictionary<string, string>
        {
            ["Id"] = reader["Id"]?.ToString() ?? "",
            ["Username"] = reader["Username"]?.ToString() ?? "",
            ["IsAdmin"] = (reader["IsAdmin"]?.ToString() == "1") ? "Yes" : "No",
            ["CreatedAt"] = reader["CreatedAt"]?.ToString() ?? ""
        });
    }

    return Results.Ok(new { ok = true, users });
});

app.MapGet("/api/analysis", async (HttpContext ctx) =>
{
    try
    {
        // 1. Compute IST and current workdone week/month
        DateTime ist;
        try
        {
            ist = GetNetworkTimeIst();
        }
        catch
        {
            ist = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow,
                TimeZoneInfo.FindSystemTimeZoneById("India Standard Time"));
        }

        string currentWeek, currentMonth, fallbackReason = null;
        try
{
    (currentWeek, currentMonth) = GetBusinessWorkdoneWeekAndBookingMonth(ist);
}
catch (Exception ex)
{
    var weekOfMonth = ((ist.Day - 1) / 7) + 1;
    currentWeek = $"W{weekOfMonth}";
    currentMonth = ist.ToString("MMM-yy", CultureInfo.InvariantCulture);
    fallbackReason = ex.Message;
}


        using var conn = new SqliteConnection($"Data Source={DbPath}");
        await conn.OpenAsync();

        // 2. Fetch all (region, circle) uploaded for this week/month (case-insensitive)
        var uploaded = new HashSet<(string, string)>();
        using (var cmd = new SqliteCommand(
            "SELECT TRIM([Region]) Region, TRIM([Circle]) Circle FROM [KpiData] WHERE [Workdone Week]=@wk AND [Booking Month]=@bm AND [Region] IS NOT NULL AND [Circle] IS NOT NULL", conn))
        {
            cmd.Parameters.AddWithValue("@wk", currentWeek);
            cmd.Parameters.AddWithValue("@bm", currentMonth);

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var reg = reader["Region"]?.ToString() ?? "";
                var cir = reader["Circle"]?.ToString() ?? "";
                if (!string.IsNullOrWhiteSpace(reg) && !string.IsNullOrWhiteSpace(cir))
                    uploaded.Add((reg.Trim().ToUpperInvariant(), cir.Trim().ToUpperInvariant()));
            }
        }

        // 3. Build the grid using config
        var analysis = new List<Dictionary<string, object>>();
        foreach (var region in validRegions)
        {
            var expectedCircles = circlesByRegion.TryGetValue(region, out var circles)
                ? circles.Select(c => c.Trim()).Where(c => !string.IsNullOrWhiteSpace(c))
                : Enumerable.Empty<string>();

            foreach (var circle in expectedCircles)
            {
                var regionKey = region.Trim().ToUpperInvariant();
                var circleKey = circle.Trim().ToUpperInvariant();
                bool isUploaded = uploaded.Contains((regionKey, circleKey));
                analysis.Add(new Dictionary<string, object>
                {
                    { "Region", region },
                    { "Circle", circle },
                    { "Count", isUploaded ? "1" : "" },
                    { "Workdone Week", currentWeek },
                    { "Booking Month", currentMonth },
                    { "Uploaded", isUploaded ? "Yes" : "No" }
                });
            }
        }

        var result = new
        {
            ok = true,
            week = currentWeek,
            month = currentMonth,
            analysis,
            fallback = fallbackReason
        };

        await ctx.Response.WriteAsJsonAsync(result);
    }
    catch (Exception ex)
    {
        ctx.Response.StatusCode = 500;
        await ctx.Response.WriteAsJsonAsync(new { ok = false, message = "Analysis failed", error = ex.Message });
    }
});


// Build expectedCircles from circlesByRegion mapping declared above (safe against nulls)
var expectedCircles = (circlesByRegion ?? new Dictionary<string, HashSet<string>>())
    .Values
    .SelectMany(set => set ?? Enumerable.Empty<string>())
    .Select(s => s?.Trim())
    .Where(s => !string.IsNullOrEmpty(s))
    .Distinct(StringComparer.OrdinalIgnoreCase)
    .ToList();

(string, string) GetBusinessWorkdoneWeekAndBookingMonth(DateTime date)
{
    int day = date.Day, month = date.Month, year = date.Year, week = 0;
    if (month == 9 && year == 2025)
    {
        if (day >= 1 && day <= 4) week = 1;
        else if (day >= 5 && day <= 11) week = 2;
        else if (day >= 12 && day <= 18) week = 3;
        else if (day >= 19 && day <= 25) week = 4;
        else week = 5;
    }
    else
    {
        if (day >= 1 && day <= 7) week = 1;
        else if (day >= 8 && day <= 14) week = 2;
        else if (day >= 15 && day <= 21) week = 3;
        else if (day >= 22 && day <= 28) week = 4;
        else week = 5;
    }
    return ($"W{week}", date.ToString("MMM-yy", CultureInfo.InvariantCulture));
}

EnsureUserTable();
EnsureAdminUser();
app.Run();


// ===== Helpers =====
void EnsureDatabaseAndTable(List<string> columns)
{
    using var conn = new SqliteConnection($"Data Source={DbPath}");
    conn.Open();
    using var cmd = conn.CreateCommand();
    cmd.CommandText = $"DROP TABLE IF EXISTS {TableName}";
    cmd.ExecuteNonQuery();

    var colDefs = columns.Select(c => $"[{c}] TEXT");
    cmd.CommandText = $@"CREATE TABLE {TableName} (
        Id INTEGER PRIMARY KEY AUTOINCREMENT,
        {string.Join(",", colDefs)}
    )";
    cmd.ExecuteNonQuery();
}

(DateTime start, DateTime end)[] BuildSatFriWeeks(DateTime date)
{
    var firstOfMonth = new DateTime(date.Year, date.Month, 1);
    var lastOfMonth = firstOfMonth.AddMonths(1).AddDays(-1);

    int satOffset = ((int)DayOfWeek.Saturday - (int)firstOfMonth.DayOfWeek + 7) % 7;
    DateTime firstSat = firstOfMonth.AddDays(satOffset);

    var weeks = new List<(DateTime start, DateTime end)>();
    if (firstSat > firstOfMonth) weeks.Add((firstOfMonth, firstSat.AddDays(-1)));

    DateTime currentStart = firstSat;
    while (currentStart <= lastOfMonth)
    {
        DateTime currentEnd = currentStart.AddDays(6);
        if (currentEnd > lastOfMonth) currentEnd = lastOfMonth;
        weeks.Add((currentStart, currentEnd));
        currentStart = currentEnd.AddDays(1);
    }
    return weeks.ToArray();
}

(string, string) GetWeekAndMonth(DateTime date, DateTime currentIst)
{
    var weeks = BuildSatFriWeeks(date);
    int weekNum = Array.FindIndex(weeks, w => currentIst.Date >= w.start && currentIst.Date <= w.end) + 1;
    if (weekNum <= 0) weekNum = 1;
    string workWeek = "W" + weekNum;
    string bookingMonth = currentIst.ToString("MMM-yy", CultureInfo.InvariantCulture);
    return (workWeek, bookingMonth);
}

DateTime GetNetworkTimeIst()
{
    const string ntpServer = "pool.ntp.org";
    byte[] ntpData = new byte[48];
    ntpData[0] = 0x1B; // LI = 0, VN = 3, Mode = 3

    var addresses = Dns.GetHostEntry(ntpServer).AddressList;
    var ipEndPoint = new IPEndPoint(addresses[0], 123);

    using var udp = new UdpClient();
    udp.Client.ReceiveTimeout = 5000;
    udp.Connect(ipEndPoint);

    udp.Send(ntpData, ntpData.Length);

    var remote = new IPEndPoint(IPAddress.Any, 0);
    byte[] response = udp.Receive(ref remote);

    if (response.Length < 48)
        throw new InvalidOperationException("Invalid NTP response.");

    ulong intPart = ((ulong)response[40] << 24) |
                    ((ulong)response[41] << 16) |
                    ((ulong)response[42] << 8)  |
                    ((ulong)response[43]);

    ulong fracPart = ((ulong)response[44] << 24) |
                     ((ulong)response[45] << 16) |
                     ((ulong)response[46] << 8)  |
                     ((ulong)response[47]);

    const ulong seventyYears = 2208988800UL;
    ulong seconds = intPart - seventyYears;
    double milliseconds = (fracPart / 4294967296.0) * 1000.0;

    DateTime utc = DateTime.UnixEpoch
        .AddSeconds(seconds)
        .AddMilliseconds(milliseconds);

    var istZone = TimeZoneInfo.FindSystemTimeZoneById("India Standard Time");
    return TimeZoneInfo.ConvertTimeFromUtc(utc, istZone);
}

string HashPassword(string password)
{
    // Using SHA256 (you may consider stronger methods e.g. PBKDF2, BCrypt)
    using var sha = SHA256.Create();
    var bytes = System.Text.Encoding.UTF8.GetBytes(password);
    var hash = sha.ComputeHash(bytes);
    return Convert.ToBase64String(hash);
}

bool VerifyPassword(string password, string storedHash)
{
    var hashOfInput = HashPassword(password);
    return StringComparer.Ordinal.Compare(hashOfInput, storedHash) == 0;
}

void EnsureUserTable()
{
    using var conn = new SqliteConnection($"Data Source={DbPath}");
    conn.Open();
    using var cmd = conn.CreateCommand();
    cmd.CommandText = @"
        CREATE TABLE IF NOT EXISTS Users (
            Id INTEGER PRIMARY KEY AUTOINCREMENT,
            Username TEXT UNIQUE NOT NULL,
            PasswordHash TEXT NOT NULL,
            IsAdmin INTEGER NOT NULL DEFAULT 0,
            CreatedAt DATETIME DEFAULT CURRENT_TIMESTAMP
        );";
    cmd.ExecuteNonQuery();
}

void EnsureAdminUser()
{
    using var conn = new SqliteConnection($"Data Source={DbPath}");
    conn.Open();

    var cmd = conn.CreateCommand();
    cmd.CommandText = "SELECT COUNT(*) FROM Users";
    long count = (long)cmd.ExecuteScalar();

    if (count == 0)
    {
        string adminUser = "admin";
        string adminPass = "admin123";
        string hash = HashPassword(adminPass);

        var insertCmd = conn.CreateCommand();
        insertCmd.CommandText = "INSERT INTO Users (Username, PasswordHash, IsAdmin) VALUES (@user, @pass, 1)";
        insertCmd.Parameters.AddWithValue("@user", adminUser);
        insertCmd.Parameters.AddWithValue("@pass", hash);
        insertCmd.ExecuteNonQuery();

        Console.WriteLine("Default admin user created: admin/admin123");
    }
}

List<string> ValidateKpiRow(Dictionary<string, string> dict, HashSet<string> existingKeys, string? currentId = null)
{
    var errors = new List<string>();

    string Get(string key) => dict.TryGetValue(key, out var val) ? val?.Trim() ?? "" : "";

    // Mandatory fields as per your schema
    string[] mandatoryFields = {
        "Region", "Circle", "Project Name (As per PMS)", "Project Name (As per Project Team)", "Customer",
        "Customer Qty", "Site ID", "SLI Code", "Customer SLI", "Customer Rate", "Customer Amount",
        "Date", "Work Done by", "Partner Name", "Partner SLI", "Partner Qty", "Partner Rate", "Partner Amount"
    };

    foreach (var field in mandatoryFields)
    {
        if (string.IsNullOrWhiteSpace(Get(field)))
            errors.Add($"Mandatory '{field}' missing.");
    }

    var validRegions = new HashSet<string> { "East", "West", "North", "South" };
    var circlesByRegion = new Dictionary<string, HashSet<string>>()
    {
        ["North"] = new HashSet<string> { "DL", "UPW", "UPE" },
        ["East"] = new HashSet<string> { "OD", "WB", "BH", "NESA", "ASM" },
        ["West"] = new HashSet<string> { "MPCG", "MH", "MUM", "RAJ", "GJ" },
        ["South"] = new HashSet<string> { "TN", "APTL", "KL", "KK" }
    };

    var region = Get("Region");
    var circle = Get("Circle");

    if (!validRegions.Contains(region))
        errors.Add("Region invalid.");
    else if (!circlesByRegion.TryGetValue(region, out var validCircles) || !validCircles.Contains(circle))
        errors.Add($"Circle '{circle}' invalid for region '{region}'.");

    var validWorkDoneBy = new HashSet<string> { "Partner", "RVS" };

    var workDoneBy = Get("Work Done by");
    if (!validWorkDoneBy.Contains(workDoneBy))
        errors.Add("Work Done by invalid.");

    var partnerName = Get("Partner Name");
    var partnerSLI = Get("Partner SLI");

    if (workDoneBy == "Partner")
    {
        if (partnerName.Equals("NA", StringComparison.OrdinalIgnoreCase))
            errors.Add("Partner Name cannot be NA for Partner.");
        if (partnerSLI.Equals("NA", StringComparison.OrdinalIgnoreCase))
            errors.Add("Partner SLI cannot be NA for Partner.");
    }
    else if (workDoneBy == "RVS")
    {
        if (!partnerName.Equals("NA", StringComparison.OrdinalIgnoreCase))
            errors.Add("Partner Name must be NA for RVS.");
        if (!partnerSLI.Equals("NA", StringComparison.OrdinalIgnoreCase))
            errors.Add("Partner SLI must be NA for RVS.");
    }

    if (!int.TryParse(Get("Customer Qty"), out int custQty))
        errors.Add("Customer Qty must be digits.");
    if (!decimal.TryParse(Get("Customer Rate"), out decimal custRate))
        errors.Add("Customer Rate invalid.");
    if (!decimal.TryParse(Get("Customer Amount"), out decimal custAmount))
        errors.Add("Customer Amount invalid.");
    else if (Math.Round(custQty * custRate, 2) != Math.Round(custAmount, 2))
        errors.Add("Customer Amount mismatch with Qty*Rate.");

    if (!decimal.TryParse(Get("Partner Qty"), out decimal partnerQty))
        errors.Add("Partner Qty invalid.");
    if (!decimal.TryParse(Get("Partner Rate"), out decimal partnerRate))
        errors.Add("Partner Rate invalid.");
    if (!decimal.TryParse(Get("Partner Amount"), out decimal partnerAmount))
        errors.Add("Partner Amount invalid.");
    else
    {
        if (workDoneBy == "Partner" && Math.Round(partnerQty * partnerRate, 2) != Math.Round(partnerAmount, 2))
            errors.Add("Partner Amount mismatch with Qty*Rate.");
        if (workDoneBy == "RVS" && (partnerQty != 0 || partnerRate != 0 || partnerAmount != 0))
            errors.Add("Partner Qty/Rate/Amount must be 0 for RVS.");
    }

    var siteId = Get("Site ID");
    if (!Regex.IsMatch(siteId, @"^[\w\-@#]+$"))
        errors.Add("Site ID invalid format.");

    var dateStr = Get("Date");

    if (!DateTime.TryParseExact(dateStr, "dd-MMM-yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out var dateVal))
    {
        errors.Add("Date format invalid, must be DD-MMM-YYYY.");
    }
    else if (dateVal.Date > DateTime.Now.Date)
    {
        errors.Add("Date cannot be in the future.");
    }
    else
    {
        // Optional: If you want to compute Workdone Week and Booking Month here,
        // add code to update dict if it’s passed and expected.
        // (string workWeek, string bookingMonth) = GetBusinessWorkdoneWeekAndBookingMonth(dateVal);
        // dict["Workdone Week"] = workWeek;
        // dict["Booking Month"] = bookingMonth;
    }

    // Duplicate key check
    var compositeKey = string.Join("||", new[]
    {
        region, circle, Get("Project Name (As per PMS)"), Get("Project Name (As per Project Team)"),
        Get("Customer"), Get("Customer Qty"), siteId, Get("Site ID as per Suffex"),
        Get("SLI Code"), Get("Customer SLI"), Get("Customer Rate"),
        Get("Customer Amount"), dateStr
    });

    if (existingKeys.Contains(compositeKey) && string.IsNullOrEmpty(currentId))
        errors.Add("Duplicate record detected.");

    return errors;
}
