using Grpc.Core;
using Grpc.Core.Interceptors;
using DataProtectionTool.Contracts;

namespace DataProtectionTool.ControlCenter.Interceptors;

public class SecretValidationInterceptor : Interceptor
{
    private readonly ILogger<SecretValidationInterceptor> _logger;

    public SecretValidationInterceptor(ILogger<SecretValidationInterceptor> logger)
    {
        _logger = logger;
    }

    public override async Task DuplexStreamingServerHandler<TRequest, TResponse>(
        IAsyncStreamReader<TRequest> requestStream,
        IServerStreamWriter<TResponse> responseStream,
        ServerCallContext context,
        DuplexStreamingServerMethod<TRequest, TResponse> continuation)
    {
        ValidateSecret(context);
        await continuation(requestStream, responseStream, context);
    }

    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        UnaryServerMethod<TRequest, TResponse> continuation)
    {
        ValidateSecret(context);
        return await continuation(request, context);
    }

    public override async Task ServerStreamingServerHandler<TRequest, TResponse>(
        TRequest request,
        IServerStreamWriter<TResponse> responseStream,
        ServerCallContext context,
        ServerStreamingServerMethod<TRequest, TResponse> continuation)
    {
        ValidateSecret(context);
        await continuation(request, responseStream, context);
    }

    public override async Task<TResponse> ClientStreamingServerHandler<TRequest, TResponse>(
        IAsyncStreamReader<TRequest> requestStream,
        ServerCallContext context,
        ClientStreamingServerMethod<TRequest, TResponse> continuation)
    {
        ValidateSecret(context);
        return await continuation(requestStream, context);
    }

    private void ValidateSecret(ServerCallContext context)
    {
        var secret = context.RequestHeaders.GetValue(SharedSecret.MetadataKey);

        if (secret != SharedSecret.Value)
        {
            _logger.LogWarning("Rejected gRPC call from {Peer} — invalid or missing shared secret", context.Peer);
            throw new RpcException(new Status(StatusCode.Unauthenticated, "Invalid shared secret"));
        }
    }
}
