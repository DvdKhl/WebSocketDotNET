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
		private ConcurrentDictionary<WSClientInfo, MemoryStream> pendingMessages = new ConcurrentDictionary<WSClientInfo, MemoryStream>();

		public void Handle(WSFrameInfo frameInfo) {
			if(frameInfo.IsFinal && frameInfo.OpCode != WSFrameInfoOpCode.Continuation) {
				OnMessageReceived(frameInfo.Client, Encoding.UTF8.GetString(frameInfo.Buffer, frameInfo.DataOffset, frameInfo.DataLength));

			} else {
				MemoryStream stream;
				bool success;

				if(frameInfo.IsFinal) {
					success = pendingMessages.TryRemove(frameInfo.Client, out stream);
				} else {
					success = pendingMessages.TryGetValue(frameInfo.Client, out stream);
				}
				if(!success && !pool.TryDequeue(out stream)) {
					stream = new MemoryStream(1024);
					if(!pendingMessages.TryAdd(frameInfo.Client, stream)) throw new NotSupportedException("Cannot handle more than one message concurrently");
				}

				stream.Write(frameInfo.Buffer, frameInfo.BufferOffset, frameInfo.BufferLength);

				if(frameInfo.IsFinal) {
					OnMessageReceived(frameInfo.Client, Encoding.UTF8.GetString(stream.GetBuffer(), 0, (int)stream.Length));

					stream.Position = 0;
					stream.SetLength(0);
					pool.Enqueue(stream);

				}
			}

		}

		protected void OnMessageReceived(WSClientInfo clientInfo, string msg) {
			var messageReceived = MessageReceived;
			if(messageReceived != null) messageReceived(clientInfo, msg);
		}


		public static StringPayloadHandler Instance { get { return instance.Value; } }
		private static readonly Lazy<StringPayloadHandler> instance =
			new Lazy<StringPayloadHandler>(() => new StringPayloadHandler(), true);

	}


}
