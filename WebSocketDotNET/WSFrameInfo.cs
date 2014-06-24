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
		internal byte[] Mask { get; set; }
		internal long ProcessedPayloadBytes { get; set; }
		internal long BytesTransferred { get; set; }
		internal long MessageLength { get; set; }
		//internal long MessagePosition { get; set; }
		internal long MessageBytesTransferred { get; set; }


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

		public void GetSAEABuffer(SocketAsyncEventArgs saea) {
			Buffer = saea.Buffer;
			BufferOffset = saea.Offset;
			BufferLength = saea.Count;
		}

		public WSFrameInfo() {
			Mask = new byte[4];
			OpCode = WSFrameInfoOpCode.None;
		}

		internal void Reset() {
			Action = WSFrameInfoAction.None;

			HeaderProcessed = false;
			IsMasked = false;
			Mask[0] = Mask[1] = Mask[2] = Mask[3] = 0;
			ProcessedPayloadBytes = 0;
			BytesTransferred = 0;
			MessageLength = 0;
			MessageBytesTransferred = 0;

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
		}

		public override string ToString() {
			return string.Format(
				"WSFrameInfo(HeaderProcessed={0} IsMasked={1} BitConverter.ToUInt32(Mask, 0)={2} ProcessedPayloadBytes={3} BytesTransferred={4} MessageLength={5} MessageBytesTransferred={6} PayloadHandler={7} SegmentGenerator={8} Version={9} OpCode={10} IsFinal={11} PayloadLength={12} HeaderLength={13} Buffer.Length={14} BufferOffset={15} BufferPosition={16} BufferLength={17} DataOffset={18} DataLength={19} Client={20})",
				HeaderProcessed, IsMasked, BitConverter.ToUInt32(Mask, 0),
				ProcessedPayloadBytes, BytesTransferred,
				MessageLength, MessageBytesTransferred,
				PayloadHandler, SegmentGenerator,
				Version, OpCode, IsFinal, PayloadLength, HeaderLength,
				Buffer.Length, BufferOffset, BufferPosition, BufferLength,
				DataOffset, DataLength,
				Client.Id
			);
		}
	}

	public enum WSFrameInfoAction {
		None, Handshake, Receive, Send
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
