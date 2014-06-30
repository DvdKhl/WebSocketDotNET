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

		public object Add(string str, int offset, int length) {
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

			return stringSendState;
		}

		public int Read(WSFrameInfo frameInfo, out bool completed) {
			var stringSendState = frameInfo.Tag as StringSendState;

			if(stringSendState == null) {
				throw new InvalidOperationException("No pending messages for this client/generator pair");
			}

			int bytesUsed;
			if(stringSendState.ByteCount == -1 || stringSendState.ByteCount > frameInfo.DataLength) {
				if(stringSendState.Encoder == null) {
					lock(pool) stringSendState.Encoder = new UTF8Encoding().GetEncoder();
				}

				unsafe {
					fixed(byte* bPtr = frameInfo.Buffer)
					fixed(char* strPtr = stringSendState.Value) {
						int charsUsed;

						stringSendState.Encoder.Convert(
							strPtr + stringSendState.Position,
							stringSendState.Value.Length - stringSendState.Position,
							bPtr + frameInfo.DataOffset, frameInfo.DataLength,
							false, out charsUsed, out bytesUsed, out completed
						);
						stringSendState.Position += charsUsed;
					}
				}

			} else {
				bytesUsed = Encoding.UTF8.GetBytes(
					stringSendState.Value,
					stringSendState.Offset, stringSendState.Length,
					frameInfo.Buffer, frameInfo.DataOffset
				);
				completed = true;
			}

			if(completed) {
				stringSendState.Encoder = null;
				stringSendState.Value = null;
				frameInfo.Tag = null;

				pool.Enqueue(stringSendState);
			}


			return bytesUsed;
		}

		public int Length(WSFrameInfo frameInfo) {
			var stringSendState = frameInfo.Tag as StringSendState;
			if(stringSendState == null) {
				throw new InvalidOperationException("No pending messages for this client/generator pair");
			}

			return stringSendState.ByteCount == -1 ? 0 : stringSendState.ByteCount;
		}

		public WSFrameInfoOpCode Type(WSFrameInfo frameInfo) { return WSFrameInfoOpCode.Text; }


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
