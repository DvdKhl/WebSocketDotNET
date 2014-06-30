using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WebSocket.PayloadHandlers {
	public class StringPayloadHandler : IPayloadHandler {
		public event Action<WSClientInfo, string> MessageReceived;

		private ConcurrentQueue<MemoryStream> pool = new ConcurrentQueue<MemoryStream>();

		public void Handle(WSFrameInfo frameInfo) {
			var isLastBlock = frameInfo.IsFinal && frameInfo.UnmaskedBytes == frameInfo.PayloadLength;

			if(isLastBlock && frameInfo.Tag == null && frameInfo.OpCode != WSFrameInfoOpCode.Continuation) {
				var messageReceived = MessageReceived;
				if(messageReceived != null) {
					messageReceived(frameInfo.Client, Encoding.UTF8.GetString(frameInfo.Buffer, frameInfo.DataOffset, frameInfo.DataLength));
				}

			} else {
				var stream = frameInfo.Tag as MemoryStream;
				if(stream == null && !pool.TryDequeue(out stream)) {
					stream = new MemoryStream(1024);
				}

				stream.Write(frameInfo.Buffer, frameInfo.DataOffset, frameInfo.DataLength);

				if(isLastBlock) {
					var messageReceived = MessageReceived;
					if(messageReceived != null) {
						messageReceived(frameInfo.Client, Encoding.UTF8.GetString(stream.GetBuffer(), 0, (int)stream.Length));
					}

					stream.Position = 0;
					stream.SetLength(0);
					pool.Enqueue(stream);
					frameInfo.Tag = null;

				} else {
					frameInfo.Tag = stream;
				}
			}
		}



		public static StringPayloadHandler Instance { get { return instance.Value; } }
		private static readonly Lazy<StringPayloadHandler> instance =
			new Lazy<StringPayloadHandler>(() => new StringPayloadHandler(), true);

	}


}
