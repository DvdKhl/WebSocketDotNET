using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WebSocket.PayloadHandlers {
	public interface IPayloadHandler {
		void Handle(WSFrameInfo frameInfo);
	}
}
