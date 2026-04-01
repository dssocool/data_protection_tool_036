FROM node:22-alpine AS frontend-build
WORKDIR /app/frontend
COPY DataProtectionTool.ControlCenter/frontend/package.json DataProtectionTool.ControlCenter/frontend/package-lock.json* ./
RUN npm ci
COPY DataProtectionTool.ControlCenter/frontend/ ./
RUN npm run build -- --outDir /app/wwwroot

FROM mcr.microsoft.com/dotnet/sdk:9.0 AS dotnet-build
WORKDIR /src
COPY DataProtectionTool.Contracts/DataProtectionTool.Contracts.csproj DataProtectionTool.Contracts/
COPY DataProtectionTool.ControlCenter/DataProtectionTool.ControlCenter.csproj DataProtectionTool.ControlCenter/
RUN dotnet restore DataProtectionTool.ControlCenter/DataProtectionTool.ControlCenter.csproj
COPY DataProtectionTool.Contracts/ DataProtectionTool.Contracts/
COPY DataProtectionTool.ControlCenter/ DataProtectionTool.ControlCenter/
COPY --from=frontend-build /app/wwwroot DataProtectionTool.ControlCenter/wwwroot/
RUN dotnet publish DataProtectionTool.ControlCenter/DataProtectionTool.ControlCenter.csproj \
    -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS runtime
WORKDIR /app
COPY --from=dotnet-build /app/publish .
EXPOSE 5000 5001
ENTRYPOINT ["dotnet", "DataProtectionTool.ControlCenter.dll"]
