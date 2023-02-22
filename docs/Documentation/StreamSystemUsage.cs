// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System.Net;
using System.Net.Security;
using RabbitMQ.Stream.Client;

namespace Documentation;

public class StreamSystemUsage
{
    // tag::create-simple[]
    private static async Task CreateSimple()
    {
        var streamSystem = await StreamSystem.Create( // <1>
            new StreamSystemConfig()
        ).ConfigureAwait(false);


        await streamSystem.Close().ConfigureAwait(false); // <2>
    }
    // end::create-simple[]


    // tag::create-multi-endpoints[]
    private static async Task CreateMultiEndPoints()
    {
        var streamSystem = await StreamSystem.Create(
            new StreamSystemConfig()
            {
                UserName = "guest",
                Password = "guest",
                Endpoints = new List<EndPoint> // <1>
                {
                    new IPEndPoint(IPAddress.Parse("192.168.5.12"), 5552),
                    new IPEndPoint(IPAddress.Parse("192.168.5.18"), 5552),
                }
            }
        ).ConfigureAwait(false);
        await streamSystem.Close().ConfigureAwait(false); // <2>
    }
    // end::create-multi-endpoints[]

    // tag::create-tls[]
    private static async Task CreateTls()
    {
        var streamSystem = await StreamSystem.Create(
            new StreamSystemConfig()
            {
                UserName = "guest",
                Password = "guest",
                Ssl = new SslOption() // <1>
                {
                    Enabled = true,
                    ServerName = "rabbitmq-stream",
                    CertPath = "/path/to/cert.pem", // <2>
                    CertPassphrase = "Password",
                }
            }
        ).ConfigureAwait(false);
        await streamSystem.Close().ConfigureAwait(false); // <2>
    }
    // end::create-tls[]


    // tag::create-tls-trust[]
    private static async Task CreateTlsTrust()
    {
        var streamSystem = await StreamSystem.Create(
            new StreamSystemConfig()
            {
                UserName = "guest",
                Password = "guest",
                Ssl = new SslOption() 
                {
                    Enabled = true,
                    AcceptablePolicyErrors = SslPolicyErrors.RemoteCertificateNotAvailable | // <1>
                                             SslPolicyErrors.RemoteCertificateChainErrors |
                                             SslPolicyErrors.RemoteCertificateNameMismatch
                }
            }
        ).ConfigureAwait(false);
        await streamSystem.Close().ConfigureAwait(false); 
    }
    // end::create-tls-trust[]


    // tag::create-address-resolver[]
    private static async Task CreateAddressResolver()
    {
        var addressResolver = new AddressResolver(new IPEndPoint(IPAddress.Parse("xxx.xxx.xxx"), 5552)); // <1>

        var streamSystem = await StreamSystem.Create(
            new StreamSystemConfig()
            {
                UserName = "myuser",
                Password = "mypassword",
                AddressResolver = addressResolver, // <2>
                Endpoints = new List<EndPoint> {addressResolver.EndPoint} // <3>
            }
        ).ConfigureAwait(false);


        await streamSystem.Close().ConfigureAwait(false);
    }
    // end::create-address-resolver[]

    // tag::stream-creation[]
    private static async Task CreateStream()
    {
        var streamSystem = await StreamSystem.Create(
            new StreamSystemConfig()
        ).ConfigureAwait(false);

        await streamSystem.CreateStream( // <1>
            new StreamSpec("my-stream")
        ).ConfigureAwait(false);
        await streamSystem.Close().ConfigureAwait(false); // <2>
    }
    // end::stream-creation[]
    
    // tag::stream-deletion[]
    private static async Task DeleteStream()
    {
        var streamSystem = await StreamSystem.Create(
            new StreamSystemConfig()
        ).ConfigureAwait(false);

        await streamSystem.DeleteStream("my-stream").ConfigureAwait(false); // <1>
        await streamSystem.Close().ConfigureAwait(false);
    }
    // end::stream-deletion[]
    
    
    // tag::stream-creation-retention[]
    private static async Task CreateStreamRetentionLen()
    {
        var streamSystem = await StreamSystem.Create(
            new StreamSystemConfig()
        ).ConfigureAwait(false);

        await streamSystem.CreateStream( 
            new StreamSpec("my-stream")
            {
                MaxLengthBytes = 10_737_418_240, // <1>
                MaxSegmentSizeBytes = 524_288_000 // <2>
            }
        ).ConfigureAwait(false);
        await streamSystem.Close().ConfigureAwait(false); 
    }
    // end::stream-creation-retention[]
    
    
    // tag::stream-creation-time-based-retention[]
    private static async Task CreateStreamRetentionAge()
    {
        var streamSystem = await StreamSystem.Create(
            new StreamSystemConfig()
        ).ConfigureAwait(false);

        await streamSystem.CreateStream( 
            new StreamSpec("my-stream")
            {
                MaxAge = TimeSpan.FromHours(6),  // <1>
                MaxSegmentSizeBytes = 524_288_000 // <2>
            }
        ).ConfigureAwait(false);
        await streamSystem.Close().ConfigureAwait(false); 
    }
    // end::stream-creation-time-based-retention[]
    
}
