using WebSocket.PayloadHandlers;
using WebSocket.SegmentGenerators;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Net.Security;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using WebSocket.Streams;

namespace WebSocket {
	public class WSClientInfo {
		internal WebSocketServer Server { get; set; }

		internal Guid Id { get; set; }
		internal Socket Socket { get; set; }

		/// <summary>Iff true the handshake was successfull</summary>
		public bool IsConnected { get; internal set; }


		internal WSSecureStream SecureStream { get; set; }

		/// <summary>The relative Uri the client used to connect</summary>
		public string Uri { get; internal set; }

		/// <summary>The queries the client used in the WS-Uri</summary>
		public ReadOnlyDictionary<string, string> Queries { get; set; }


		public IPayloadHandler DefaultPayloadHandler { get; set; }


		public void SendAsBinary(byte[] data, bool copy = false) { SendAsBinary(data, 0, data.Length, copy); }
		public void SendAsBinary(byte[] data, int offset, int count, bool copy = false) {
			throw new NotImplementedException();
			Send(null);
		}

		public void SendAsString(string str) { SendAsString(str, 0, str.Length); }
		public void SendAsString(string str, int offset, int count) {
			StringSegmentGenerator.Instance.Add(this, str, 0, str.Length);
			Send(StringSegmentGenerator.Instance);
		}

		public void Send(ISegmentGenerator generator) { Server.Send(this, generator); }


		public string HttpVersion { get; internal set; }
		public string Host { get; internal set; }
		public string Key { get; internal set; }
		public string Origin { get; internal set; }
		public string WebSocketVersion { get; internal set; }
		public string Protocol { get; internal set; }
		public string Extensions { get; internal set; }
		public string UserAgent { get; internal set; }

		public WSClientInfo() {
			Id = Guid.NewGuid();
		}
	}
}
