FROM node:22-alpine AS frontend-build
WORKDIR /app/frontend
COPY DataProtectionTool.ControlCenter.HttpServer/frontend/package.json DataProtectionTool.ControlCenter.HttpServer/frontend/package-lock.json* ./
RUN npm ci
COPY DataProtectionTool.ControlCenter.HttpServer/frontend/ ./
RUN npm run build -- --outDir /app/wwwroot

FROM mcr.microsoft.com/dotnet/sdk:9.0 AS dotnet-build
WORKDIR /src
COPY DataProtectionTool.Contracts/DataProtectionTool.Contracts.csproj DataProtectionTool.Contracts/
COPY DataProtectionTool.ControlCenter.HttpServer/DataProtectionTool.ControlCenter.HttpServer.csproj DataProtectionTool.ControlCenter.HttpServer/
RUN dotnet restore DataProtectionTool.ControlCenter.HttpServer/DataProtectionTool.ControlCenter.HttpServer.csproj
COPY DataProtectionTool.Contracts/ DataProtectionTool.Contracts/
COPY DataProtectionTool.ControlCenter.HttpServer/ DataProtectionTool.ControlCenter.HttpServer/
COPY --from=frontend-build /app/wwwroot DataProtectionTool.ControlCenter.HttpServer/wwwroot/
RUN dotnet publish DataProtectionTool.ControlCenter.HttpServer/DataProtectionTool.ControlCenter.HttpServer.csproj \
    -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS runtime
WORKDIR /app
COPY --from=dotnet-build /app/publish .
EXPOSE 8190
ENV RpcServer__Address=""
ENV AzureTableStorage__ConnectionString=""
ENV AzureBlobStorage__StorageAccount=""
ENV AzureBlobStorage__Container=""
ENV AzureBlobStorage__AccessKey=""
ENV AzureBlobStorage__PreviewContainer=""
ENV DataEngine__EngineUrl=""
ENV DataEngine__AuthorizationToken=""
ENV DataEngine__EnvironmentId=""
ENV DataEngine__ConnectorId=""
ENV DataEngine__ProfileSetId=""
ENTRYPOINT ["dotnet", "DataProtectionTool.ControlCenter.HttpServer.dll"]
