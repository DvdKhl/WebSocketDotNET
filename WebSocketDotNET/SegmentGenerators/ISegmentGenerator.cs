using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WebSocket.SegmentGenerators {
	public interface ISegmentGenerator {
		int Length(WSClientInfo clientInfo);
		WSFrameInfoOpCode Type(WSClientInfo clientInfo);
		int Read(WSClientInfo clientInfo, byte[] buffer, int offset, int length, out bool complete);



	}
}
