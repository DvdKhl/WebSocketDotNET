using WebSocket.PayloadHandlers;
using WebSocket.SegmentGenerators;
using WebSocket.Streams;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Net.Security;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace WebSocket {
	public class WSClientInfo {
		internal WebSocketServer Server { get; set; }

		internal Guid Id { get; set; }
		internal Socket Socket { get; set; }

		/// <summary>Iff true the handshake was successfull</summary>
		public bool IsConnected { get; internal set; }
		internal int ConnectedFlag;

		internal WSSecureStream SecureStream { get; set; }

		/// <summary>The relative Uri the client used to connect</summary>
		public string Uri { get; internal set; }

		/// <summary>The queries the client used in the WS-Uri</summary>
		public ReadOnlyDictionary<string, string> Queries { get; set; }


		public IPayloadHandler DefaultPayloadHandler { get; set; }

		public void Send(ISegmentGenerator generator, object tag) { Server.Send(this, generator, tag); }

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

	public static class WSClientInfoEx {
		public static void SendAsBinary(this WSClientInfo clientInfo, byte[] data, bool copy = false) { SendAsBinary(clientInfo, data, 0, data.Length, copy); }
		public static void SendAsBinary(this WSClientInfo clientInfo, byte[] data, int offset, int count, bool copy = false) {
			throw new NotImplementedException();
			clientInfo.Send(null, null);
		}

		public static void SendAsString(this WSClientInfo clientInfo, string str) { SendAsString(clientInfo, str, 0, str.Length); }
		public static void SendAsString(this WSClientInfo clientInfo, string str, int offset, int count) {
			var genObj = StringSegmentGenerator.Instance.Add(str, 0, str.Length);
			clientInfo.Send(StringSegmentGenerator.Instance, genObj);
		}

	}
}
