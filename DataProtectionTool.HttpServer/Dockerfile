FROM node:22-alpine AS frontend-build
WORKDIR /app/frontend
COPY DataProtectionTool.HttpServer/frontend/package.json DataProtectionTool.HttpServer/frontend/package-lock.json* ./
RUN npm ci
COPY DataProtectionTool.HttpServer/frontend/ ./
RUN npm run build -- --outDir /app/wwwroot

FROM mcr.microsoft.com/dotnet/sdk:9.0 AS dotnet-build
WORKDIR /src
COPY DataProtectionTool.Contracts/DataProtectionTool.Contracts.csproj DataProtectionTool.Contracts/
COPY DataProtectionTool.HttpServer/DataProtectionTool.HttpServer.csproj DataProtectionTool.HttpServer/
RUN dotnet restore DataProtectionTool.HttpServer/DataProtectionTool.HttpServer.csproj
COPY DataProtectionTool.Contracts/ DataProtectionTool.Contracts/
COPY DataProtectionTool.HttpServer/ DataProtectionTool.HttpServer/
COPY --from=frontend-build /app/wwwroot DataProtectionTool.HttpServer/wwwroot/
RUN dotnet publish DataProtectionTool.HttpServer/DataProtectionTool.HttpServer.csproj \
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
ENTRYPOINT ["dotnet", "DataProtectionTool.HttpServer.dll"]
