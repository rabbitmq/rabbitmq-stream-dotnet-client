// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System.Collections.Generic;

namespace RabbitMQ.Stream.Client.Hash;
// https://github.com/darrenkopp/murmurhash-net/blob/master/MurmurHash/Murmur32.cs
internal class Murmur32ManagedX86 : Murmur3
{
    public Murmur32ManagedX86(uint seed = 0)
        : base(seed)
    {
    }

    protected override void HashCore(byte[] array, int ibStart, int cbSize)
    {
        Length += cbSize;
        Body(array, ibStart, cbSize);
    }

#if NETFX45
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
    private void Body(byte[] data, int start, int length)
    {
        var remainder = length & 3;
        var alignedLength = start + (length - remainder);

        for (var i = start; i < alignedLength; i += 4)
        {
            H1 = (((H1 ^ (((data.ToUInt32(i) * C1).RotateLeft(15)) * C2)).RotateLeft(13)) * 5) + 0xe6546b64;
        }

        if (remainder > 0)
        {
            Tail(data, alignedLength, remainder);
        }
    }

#if NETFX45
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
    private void Tail(IReadOnlyList<byte> tail, int position, int remainder)
    {
        // create our keys and initialize to 0
        uint k1 = 0;

        // determine how many bytes we have left to work with based on length
        switch (remainder)
        {
            case 3:
                k1 ^= (uint)tail[position + 2] << 16;
                goto case 2;
            case 2:
                k1 ^= (uint)tail[position + 1] << 8;
                goto case 1;
            case 1:
                k1 ^= tail[position];
                break;
        }

        H1 ^= (k1 * C1).RotateLeft(15) * C2;
    }
}
