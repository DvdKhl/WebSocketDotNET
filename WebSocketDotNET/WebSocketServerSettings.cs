using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace WebSocket {
	public class WebSocketServerSettings {
		public AddressFamily AddressFamily { get; set; }
		public IPEndPoint LocalEndPoint { get; set; }
		public int MaxConcurrentConnections { get; set; }

		public int MaxConcurrentAcceptOperations { get; set; }

		public int TransferBufferSize { get; set; } //At least 14 byte (max header length)

		public int MaxConcurrentTransferOperations { get; set; }

		public int Backlog { get; set; }

		public bool UseTLS { get; set; }

		public object ServerCertificatePath { get; set; }
	}
}
