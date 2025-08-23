# =========================
# 1) Build stage
# =========================
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY . .

# If your .csproj is in the repo root:
RUN dotnet restore "./KpiWeb.csproj"
RUN dotnet publish "./KpiWeb.csproj" -c Release -o /app

# =========================
# 2) Runtime stage
# =========================
FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app
COPY --from=build /app .

ENV ASPNETCORE_ENVIRONMENT=Production
ENV ASPNETCORE_URLS=http://0.0.0.0:$PORT

CMD ["dotnet", "KpiWeb.dll"]
