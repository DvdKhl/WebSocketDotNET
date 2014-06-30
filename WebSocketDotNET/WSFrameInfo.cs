using WebSocket.PayloadHandlers;
using WebSocket.SegmentGenerators;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace WebSocket {
	public class WSFrameInfo {
		internal WSFrameInfoAction Action { get; set; }
		internal bool HeaderProcessed { get; set; }
		internal bool IsMasked { get; set; }
		internal ulong Mask { get; set; }
		internal long UnmaskedBytes { get; set; }
		internal long MessagePayloadLength { get; set; }
		internal long MessagePayloadBytesTransferred { get; set; }


		internal IPayloadHandler PayloadHandler { get; set; }
		internal ISegmentGenerator SegmentGenerator { get; set; }

		public WSFrameInfoVersion Version { get; internal set; }
		public WSFrameInfoOpCode OpCode { get; internal set; }
		public bool IsFinal { get; internal set; }

		public long PayloadLength { get; internal set; }
		public int HeaderLength { get; internal set; }
		public long TotalLength { get { return HeaderLength + PayloadLength; } }

		public byte[] Buffer { get; internal set; }
		public int BufferOffset { get; internal set; }
		public int BufferPosition { get; internal set; }
		public int BufferLength { get; internal set; }
		public int DataOffset { get; internal set; }
		public int DataLength { get; internal set; }

		public WSClientInfo Client { get; internal set; }
		public object Tag { get; set; }

		public void GetSAEABuffer(SocketAsyncEventArgs saea) {
			Buffer = saea.Buffer;
			BufferOffset = saea.Offset;
			BufferLength = saea.Count;
		}

		public WSFrameInfo() {
			OpCode = WSFrameInfoOpCode.None;
		}

		internal void Reset() {
			Action = WSFrameInfoAction.None;

			HeaderProcessed = false;
			IsMasked = false;
			Mask = 0;
			UnmaskedBytes = 0;
			MessagePayloadLength = 0;
			MessagePayloadBytesTransferred = 0;

			PayloadHandler = null;
			SegmentGenerator = null;

			Version = 0;
			OpCode = WSFrameInfoOpCode.None;
			IsFinal = false;

			PayloadLength = 0;
			HeaderLength = 0;

			Buffer = null;
			BufferOffset = 0;
			BufferPosition = 0;
			BufferLength = 0;
			DataOffset = 0;
			DataLength = 0;

			Client = null;
			Tag = null;
		}

		public override string ToString() {
			return string.Format(
				"WSFrameInfo(\n\tHeaderProcessed={0} IsMasked={1} Mask={2}\n\tUnmaskedBytes={3} MessageLength={4} MessagePayloadBytesTransferred={5}\n\tPayloadHandler={6} SegmentGenerator={7}\n\tVersion={8} OpCode={9} IsFinal={10} PayloadLength={11} HeaderLength={12}\n\tBuffer.Length={13} BufferOffset={14} BufferPosition={15} BufferLength={16}\n\tDataOffset={17} DataLength={18} Client={19}\n)",
				HeaderProcessed, IsMasked, Mask,
				UnmaskedBytes,
				MessagePayloadLength, MessagePayloadBytesTransferred,
				PayloadHandler, SegmentGenerator,
				Version, OpCode, IsFinal, PayloadLength, HeaderLength,
				Buffer.Length, BufferOffset, BufferPosition, BufferLength,
				DataOffset, DataLength,
				Client.Id
			);
		}
	}

	public enum WSFrameInfoAction {
		None, HandshakeReceive, HandshakeSend, Receive, Send, Close
	}

	public enum WSFrameInfoVersion {
		RSV1 = 1, RSV2 = 2, RSV3 = 4
	}
	public enum WSFrameInfoOpCode {
		None = -1,
		Continuation = 0,
		Text = 1,
		Binary = 2,
		Close = 8,
		Ping = 9,
		Pong = 10,
	}

}
