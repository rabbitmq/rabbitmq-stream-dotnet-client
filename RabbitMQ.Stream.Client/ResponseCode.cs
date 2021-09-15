namespace RabbitMQ.Stream.Client
{
    public enum ResponseCode : ushort
    {
        Ok = 1,
        StreamDoesNotExist = 2,
        SubscriptionIdAlreadyExists = 3,
        SubscriptionIdDoesNotExist = 4,
        StreamAlreadyExists = 5,
        StreamNotAvailable = 6,
        SaslMechanismNotSupported = 7,
        AuthenticationFailure = 8,
        SaslError = 9,
        SaslChallenge = 10,
        SaslAuthenticationFailureLoopback = 11,
        VirtualHostAccessFailure =  12,
        UnknownFrame = 13,
        FrameTooLarge = 14 ,
        InternalError = 15,
        AccessRefused = 16,
        PreconditionFailed = 17,
        PublisherDoesNotExist = 18 
    }
}