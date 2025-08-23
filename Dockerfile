# =========================
# 1. Build stage
# =========================
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

# copy everything into container
COPY . .

# restore dependencies and publish only the KpiWeb project
RUN dotnet restore "KpiWeb/KpiWeb.csproj"
RUN dotnet publish "KpiWeb/KpiWeb.csproj" -c Release -o /app

# =========================
# 2. Runtime stage
# =========================
FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app
COPY --from=build /app .

# set environment
ENV ASPNETCORE_ENVIRONMENT=Production
ENV ASPNETCORE_URLS=http://0.0.0.0:$PORT

# start the app
CMD ["dotnet", "KpiWeb.dll"]