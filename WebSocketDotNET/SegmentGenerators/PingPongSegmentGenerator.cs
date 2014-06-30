using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WebSocket.SegmentGenerators {
	public class PingPongSegmentGenerator : ISegmentGenerator {
		private ConcurrentQueue<PingPongData> pool = new ConcurrentQueue<PingPongData>();

		public object AddOrUpdate(bool isPing, byte[] buffer, int offset, int length) {
			if(length > 1024) throw new ArgumentException("Application data may not be greater than 1024 bytes", "length");

			PingPongData pingPongData;
			if(!pool.TryDequeue(out pingPongData)) {
				pingPongData = new PingPongData { Buffer = new byte[1024] };
			}

			pingPongData.IsPing = isPing;
			pingPongData.Length = length;
			pingPongData.Position = 0;
			Buffer.BlockCopy(buffer, offset, pingPongData.Buffer, 0, length);

			return pingPongData;
		}

		public int Length(WSFrameInfo frameInfo) {
			var pingPongData = frameInfo.Tag as PingPongData;
			if(pingPongData == null) {
				throw new InvalidOperationException("No pending pings/pongs for this client/generator pair");
			}

			return pingPongData.Length;
		}

		public WSFrameInfoOpCode Type(WSFrameInfo frameInfo) {
			var pingPongData = frameInfo.Tag as PingPongData;
			if(pingPongData == null) {
				throw new InvalidOperationException("No pending pings/pongs for this client/generator pair");
			}

			return pingPongData.IsPing ? WSFrameInfoOpCode.Ping : WSFrameInfoOpCode.Pong;
		}

		public int Read(WSFrameInfo frameInfo, out bool complete) {
			var pingPongData = frameInfo.Tag as PingPongData;
			if(pingPongData == null) {
				throw new InvalidOperationException("No pending pings/pongs for this client/generator pair");
			}

			var copyCount = Math.Min(pingPongData.Length - pingPongData.Position, frameInfo.DataLength);
			Buffer.BlockCopy(pingPongData.Buffer, pingPongData.Position, frameInfo.Buffer, frameInfo.DataOffset, copyCount);
			pingPongData.Position += copyCount;

			complete = pingPongData.Position == pingPongData.Length;


			if(complete) pool.Enqueue(pingPongData);


			return copyCount;
		}

		private class PingPongData {
			public bool IsPing;
			public byte[] Buffer;
			public int Position;
			public int Length;
		}
	}
}
