// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System;

namespace RabbitMQ.Stream.Client;

internal class Crc32
{
    private static readonly uint[] s_crcTable;

    static Crc32()
    {
        s_crcTable = new uint[256];
        const uint Polynomial = 0xEDB88320U;

        for (uint i = 0; i < 256; i++)
        {
            var crc = i;
            for (var j = 0; j < 8; j++)
            {
                if ((crc & 1) != 0)
                {
                    crc = (crc >> 1) ^ Polynomial;
                }
                else
                {
                    crc >>= 1;
                }
            }

            s_crcTable[i] = crc;
        }
    }

    internal static uint ComputeHash(Span<byte> Span)
    {

        var crc = 0xFFFFFFFFU;

        foreach (var b in Span)
        {
            crc = (crc >> 8) ^ s_crcTable[(crc & 0xFF) ^ b];
        }

        return crc ^ 0xFFFFFFFFU;
    }
}
