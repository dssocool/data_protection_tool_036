FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src
COPY DataProtectionTool.Contracts/DataProtectionTool.Contracts.csproj DataProtectionTool.Contracts/
COPY DataProtectionTool.RpcServer/DataProtectionTool.RpcServer.csproj DataProtectionTool.RpcServer/
RUN dotnet restore DataProtectionTool.RpcServer/DataProtectionTool.RpcServer.csproj
COPY DataProtectionTool.Contracts/ DataProtectionTool.Contracts/
COPY DataProtectionTool.RpcServer/ DataProtectionTool.RpcServer/
RUN dotnet publish DataProtectionTool.RpcServer/DataProtectionTool.RpcServer.csproj \
    -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS runtime
WORKDIR /app
COPY --from=build /app/publish .
EXPOSE 5000
ENV ASPNETCORE_URLS=http://+:5000
ENTRYPOINT ["dotnet", "DataProtectionTool.RpcServer.dll"]
