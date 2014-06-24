using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WebSocket.SegmentGenerators {
	public class StringSegmentGenerator : ISegmentGenerator {
		private ConcurrentQueue<StringSendState> pool = new ConcurrentQueue<StringSendState>();
		private ConcurrentDictionary<WSClientInfo, StringSendState> pending = new ConcurrentDictionary<WSClientInfo, StringSendState>();

		public void Add(WSClientInfo clientInfo, string str, int offset, int length) {
			StringSendState stringSendState;
			if(!pool.TryDequeue(out stringSendState)) stringSendState = new StringSendState();

			stringSendState.Value = str;
			stringSendState.Position = 0;
			stringSendState.Offset = offset;
			stringSendState.Length = length;

			if(str.Length < 8192) { //TODO: Make more configurable
				stringSendState.ByteCount = Encoding.UTF8.GetByteCount(str);
			} else {
				stringSendState.ByteCount = -1;
			}

			if(!pending.TryAdd(clientInfo, stringSendState)) {
				throw new NotSupportedException("Cannot send more than one message concurrently");
			}
		}

		public int Read(WSClientInfo clientInfo, byte[] buffer, int offset, int length, out bool completed) {
			StringSendState stringSendState;
			if(!pending.TryRemove(clientInfo, out stringSendState)) {
				throw new InvalidOperationException("No pending messages for this client/generator pair");
			}

			int bytesUsed;
			if(stringSendState.ByteCount == -1 || stringSendState.ByteCount > length) {
				if(stringSendState.Encoder == null) {
					lock(pending) stringSendState.Encoder = Encoding.UTF8.GetEncoder();
				}

				unsafe {
					fixed(byte* bPtr = buffer)
					fixed(char* strPtr = stringSendState.Value) {
						int charsUsed;

						stringSendState.Encoder.Convert(
							strPtr + stringSendState.Position,
							stringSendState.Value.Length - stringSendState.Position,
							bPtr + offset, length,
							false, out charsUsed, out bytesUsed, out completed
						);
						stringSendState.Position += charsUsed;
					}
				}

			} else {
				bytesUsed = Encoding.UTF8.GetBytes(
					stringSendState.Value,
					stringSendState.Offset, stringSendState.Length,
					buffer, offset
				);
				completed = true;
			}

			if(completed) {
				stringSendState.Encoder = null;
				stringSendState.Value = null;
				pool.Enqueue(stringSendState);

			} else if(!pending.TryAdd(clientInfo, stringSendState)) {
				throw new NotSupportedException("Cannot send more than one message concurrently");
			}


			return bytesUsed;
		}

		public int Length(WSClientInfo clientInfo) {
			StringSendState stringSendState;
			if(!pending.TryGetValue(clientInfo, out stringSendState)) {
				throw new InvalidOperationException("No pending messages for this client/generator pair");
			}
			return stringSendState.ByteCount == -1 ? 0 : stringSendState.ByteCount;
		}

		public WSFrameInfoOpCode Type(WSClientInfo clientInfo) { return WSFrameInfoOpCode.Text; }


		private class StringSendState {
			public Encoder Encoder;
			public string Value;
			public int Position;
			public int Offset;
			public int Length;
			public int ByteCount;
		}


		public static StringSegmentGenerator Instance { get { return instance.Value; } }
		private static readonly Lazy<StringSegmentGenerator> instance =
			new Lazy<StringSegmentGenerator>(() => new StringSegmentGenerator(), true);


	}
}
