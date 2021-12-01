using System;

namespace RabbitMQ.Stream.Client
{
    internal static class ClientExceptions
    {
        
        public static void MaybeThrowException(ResponseCode responseCode, string message)
        {
            if (responseCode != ResponseCode.Ok)
            {
                throw responseCode switch
                {
                    ResponseCode.VirtualHostAccessFailure => new VirtualHostAccessFailureException(message),
                    //TODO Implement the other responses
                    _ => new NotImplementedException()
                };
            }
        }
        
    }
    
    public class ProtocolException : Exception
    {
        protected ProtocolException(string s) : base(s)
        {

        }
    }

    public class VirtualHostAccessFailureException : ProtocolException
    {
        public VirtualHostAccessFailureException(string s) : base(s)
        {

        }
    }
}