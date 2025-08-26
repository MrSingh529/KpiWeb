# 1) Build stage (.NET 9)
FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src
COPY . .

RUN dotnet restore "./KpiWeb.csproj"
RUN dotnet publish "./KpiWeb.csproj" -c Release -o /app

# 2) Runtime stage (.NET 9)

FROM mcr.microsoft.com/dotnet/aspnet:9.0
WORKDIR /app
COPY --from=build /app .

# Bind correctly on Render
ENV ASPNETCORE_ENVIRONMENT=Production
ENV ASPNETCORE_URLS=http://0.0.0.0:$PORT

# Run the app
CMD ["dotnet", "KpiWeb.dll"]