FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src
COPY DataProtectionTool.Contracts/DataProtectionTool.Contracts.csproj DataProtectionTool.Contracts/
COPY DataProtectionTool.ControlCenter.RpcServer/DataProtectionTool.ControlCenter.RpcServer.csproj DataProtectionTool.ControlCenter.RpcServer/
RUN dotnet restore DataProtectionTool.ControlCenter.RpcServer/DataProtectionTool.ControlCenter.RpcServer.csproj
COPY DataProtectionTool.Contracts/ DataProtectionTool.Contracts/
COPY DataProtectionTool.ControlCenter.RpcServer/ DataProtectionTool.ControlCenter.RpcServer/
RUN dotnet publish DataProtectionTool.ControlCenter.RpcServer/DataProtectionTool.ControlCenter.RpcServer.csproj \
    -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS runtime
WORKDIR /app
COPY --from=build /app/publish .
EXPOSE 5000
ENV ASPNETCORE_URLS=http://+:5000
ENV AzureTableStorage__ConnectionString=""
ENV AzureBlobStorage__StorageAccount=""
ENV AzureBlobStorage__Container=""
ENV AzureBlobStorage__AccessKey=""
ENV AzureBlobStorage__PreviewContainer=""
ENTRYPOINT ["dotnet", "DataProtectionTool.ControlCenter.RpcServer.dll"]
