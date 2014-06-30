using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WebSocket.SegmentGenerators {
	public interface ISegmentGenerator {
		int Length(WSFrameInfo frameInfo);
		WSFrameInfoOpCode Type(WSFrameInfo frameInfo);
		int Read(WSFrameInfo frameInfo, out bool complete);
	}
}
