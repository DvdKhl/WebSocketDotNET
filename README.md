WebSocketDotNET
===============
Warning: This project hasn't reached a stable state yet and may unexpectedly crash, so use this for development or testing only.  
Since I'm currently working on a different project, which may at some point use this one, the development is currently halted.

What works:
- Connecting with ws://
- Receiving and sending (Not tested very well yet, so data corruption/truncation may occur)

Issues:
- If there are many clients connecting in a very short period of time the server may crash
- If the client count gets too high with high throughput, performance degrades massively