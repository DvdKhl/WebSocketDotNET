using WebSocket.PayloadHandlers;
using WebSocket.SegmentGenerators;
using WebSocket.Streams;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace WebSocket {
	public delegate IPayloadHandler PayloadHandlerRequestHandler(WSFrameInfo frameInfo);

	public class WebSocketServer {
		private static Guid operationsActivityId = Guid.NewGuid();
		protected static TraceSourceEx ts = new TraceSourceEx("DvdKhl.WebSocket.WebSocketServer", System.Diagnostics.SourceLevels.All);

		public event PayloadHandlerRequestHandler PayloadHandlerRequest;

		private byte[] comBuffer;
		private Socket listenSocket;

		private BlockingCollection<SocketAsyncEventArgs> acceptSAEAPool;
		private ConcurrentQueue<SocketAsyncEventArgs> transferSAEAPool;
		private ConcurrentQueue<WSFrameInfo> frameInfoPool;
		private ConcurrentQueue<WSFrameInfo> pendingOperations;

		private WebSocketServerSettings settings;

		private PingPongSegmentGenerator pingPongGenerator;

		private SemaphoreSlim connectionLimiter;
		private SHA1 sha1HashObj = SHA1.Create();

		public IPayloadHandler DefaultPayloadHandler { get; set; }
		public int MaxSendFrameLength { get; set; }

		private long bytesReceived = 0;

		public WebSocketServer(WebSocketServerSettings settings) {
			//MaxFramePayloadLength = 512;

			using(ts.CreateActivity("WebSocketServer Constructor")) {
				this.settings = settings;

				pingPongGenerator = new PingPongSegmentGenerator();

				listenSocket = new Socket(settings.AddressFamily, SocketType.Stream, ProtocolType.Tcp);

				connectionLimiter = new SemaphoreSlim(
					settings.MaxConcurrentConnections,
					settings.MaxConcurrentConnections
				);

				EventHandler<SocketAsyncEventArgs> acceptHandler = (s, e) => ProcessAccept(e);
				acceptSAEAPool = new BlockingCollection<SocketAsyncEventArgs>(settings.MaxConcurrentAcceptOperations);
				for(int i = 0; i < settings.MaxConcurrentAcceptOperations; i++) {
					var saea = new SocketAsyncEventArgs();
					saea.Completed += acceptHandler;
					acceptSAEAPool.Add(saea);
				}

				comBuffer = new byte[settings.MaxConcurrentTransferOperations * settings.TransferBufferSize];
				transferSAEAPool = new ConcurrentQueue<SocketAsyncEventArgs>();
				for(int i = 0; i < settings.MaxConcurrentTransferOperations; i++) {
					var saea = new SocketAsyncEventArgs();
					saea.SetBuffer(comBuffer, i * settings.TransferBufferSize, settings.TransferBufferSize);
					saea.Completed += OnIOCompleted;
					transferSAEAPool.Enqueue(saea);
				}

				frameInfoPool = new ConcurrentQueue<WSFrameInfo>();
				for(int i = 0; i < settings.MaxConcurrentConnections; i++) {
					frameInfoPool.Enqueue(new WSFrameInfo());
				}

				pendingOperations = new ConcurrentQueue<WSFrameInfo>();

				StartListen();
			}

			t = new Timer(PrintState, null, 0, 200);
		}

		private Timer t;
		private long prevBytesReceived;
		private void PrintState(object state) {
			var bw = (bytesReceived - prevBytesReceived) / 0.2 / 1024 / 1024;

			Console.WriteLine("CurCons={0:00} Pending={1:00} BytesReceived={2} BW={3:##0.00}", connectionLimiter.CurrentCount, pendingOperations.Count, bytesReceived, bw);
			prevBytesReceived = bytesReceived;
		}

		private void StartListen() {
			using(ts.CreateActivity("WebSocketServer Accept operations", true, operationsActivityId)) {

				listenSocket = new Socket(settings.LocalEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
				listenSocket.Bind(this.settings.LocalEndPoint);

				listenSocket.Listen(this.settings.Backlog);

				StartAccept();
			}
		}

		private void StartAccept() {
			using(ts.CreateActivity("WebSocketServer StartAccept", true, operationsActivityId)) {
				ts.TraceInformation("Waiting for permission to do a accept operation (Free: {0})", connectionLimiter.CurrentCount);

				ts.TraceInformation("Starting Accept operation");
				var acceptSAEA = this.acceptSAEAPool.Take();
				if(!listenSocket.AcceptAsync(acceptSAEA)) {
					ts.TraceInformation("AcceptAsync finished synchronously");
					ProcessAccept(acceptSAEA);
				}
			}
		}
		private void ProcessAccept(SocketAsyncEventArgs acceptSAEA) {
			using(ts.CreateActivity("WebSocketServer ProcessAccept", true, operationsActivityId)) {
				if(acceptSAEA.SocketError != SocketError.Success) {
					ts.TraceInformation("Accept operation didn't complete successfully");

					acceptSAEA.AcceptSocket.Close();
					acceptSAEA.AcceptSocket = null;
					acceptSAEAPool.Add(acceptSAEA);
					StartAccept();
					return;
				}

				connectionLimiter.Wait();

				ts.TraceInformation("Preparing Handshake operation");
				WSFrameInfo frameInfo;
				if(!frameInfoPool.TryDequeue(out frameInfo)) {
					ts.TraceInformation("Created WSFrameInfo");
					frameInfo = new WSFrameInfo();
				}

				frameInfo.Action = WSFrameInfoAction.HandshakeReceive;

				frameInfo.Client = new WSClientInfo {
					ConnectedFlag = 1,
					Server = this,
					Socket = acceptSAEA.AcceptSocket,
					DefaultPayloadHandler = DefaultPayloadHandler
				};

				ts.TraceInformation("Listen for next client connection");
				acceptSAEA.AcceptSocket = null;
				acceptSAEAPool.Add(acceptSAEA);
				StartAccept();

				using(ts.CreateActivity("WebSocketClient", frameInfo.Client.Id)) {
					//TOOD: Fire ClientConnected event

					if(settings.UseTLS) {
						ts.TraceInformation("Starting TLS");
						frameInfo.Client.SecureStream = new WSSecureStream(frameInfo.Client.Socket, settings.ServerCertificatePath);
						if(!frameInfo.Client.SecureStream.IsConnected) {
							ts.TraceInformation("TLS failed");
							return;
						}
					}

					SocketAsyncEventArgs receiveSAEA;
					if(transferSAEAPool.TryDequeue(out receiveSAEA)) {
						ts.TraceInformation("Starting Handshake operation");
						receiveSAEA.UserToken = frameInfo;
						InitHandshake(receiveSAEA);

					} else {
						ts.TraceInformation("Enqueuing Handshake operation");
						pendingOperations.Enqueue(frameInfo);
					}
				}
			}
		}

		private void InitHandshake(SocketAsyncEventArgs receiveSAEA) {
			var frameInfo = (WSFrameInfo)receiveSAEA.UserToken;

			using(ts.CreateActivity("WebSocketClient InitHandshake", true, frameInfo.Client.Id)) {
				receiveSAEA.Completed -= OnIOCompleted;
				receiveSAEA.Completed += OnHandshake;

				frameInfo.GetSAEABuffer(receiveSAEA);
				receiveSAEA.SetBuffer(new byte[4096], 0, 4096); //TODO?: Performance

				StartHandshake(receiveSAEA);
			}
		}
		private void StartHandshake(SocketAsyncEventArgs receiveSAEA) {
			var frameInfo = (WSFrameInfo)receiveSAEA.UserToken;
			using(ts.CreateActivity("WebSocketClient StartHandshake", true, frameInfo.Client.Id)) {
				receiveSAEA.SetBuffer(frameInfo.BufferPosition, 4096 - frameInfo.BufferPosition);

				if(!frameInfo.Client.Socket.ReceiveAsync(receiveSAEA)) {
					ts.TraceInformation("ReceiveAsync finished synchronously");
					ProcessHandshake(receiveSAEA);
				}
			}
		}
		private void OnHandshake(object sender, SocketAsyncEventArgs saea) {
			//TODO: Error handling
			ProcessHandshake(saea);
		}
		private void ProcessHandshake(SocketAsyncEventArgs handshakeSAEA) {
			var frameInfo = (WSFrameInfo)handshakeSAEA.UserToken;

			using(ts.CreateActivity("WebSocketClient ProcessHandshake", true, frameInfo.Client.Id)) {
				if(settings.UseTLS) {
					ts.TraceInformation("Decrypt data");
					frameInfo.BufferPosition += frameInfo.Client.SecureStream.DecryptInPlace(handshakeSAEA.Buffer, handshakeSAEA.Offset, handshakeSAEA.BytesTransferred);
				} else {
					frameInfo.BufferPosition += handshakeSAEA.BytesTransferred;
				}

				ts.TraceInformation("Seeking Html header end");
				int matches = 0;
				int pos = Math.Max(0, handshakeSAEA.Offset - 3); //In case of a CR LF CR | LF split 
				for(; pos < frameInfo.BufferPosition; pos++) {
					var value = handshakeSAEA.Buffer[pos];
					var isMatch = (matches == 0 || matches == 2) && value == '\r';
					isMatch = isMatch || (matches == 1 || matches == 3) && value == '\n';
					matches = isMatch ? matches + 1 : 0;

					if(matches == 4) break;
				}
				pos++;


				if(handshakeSAEA.BytesTransferred != 0 && matches != 4 && frameInfo.BufferPosition < 4096) {
					ts.TraceInformation("Html header end not found, requesting more data");
					StartHandshake(handshakeSAEA);
					return;
				}

				ts.TraceInformation("Reset handshakeSAEA to previous state (changed in InitHandshake)");
				//Reset to default completed handler which we changed in InitHandshake
				handshakeSAEA.Completed -= OnHandshake;
				handshakeSAEA.Completed += OnIOCompleted;

				var header = handshakeSAEA.Buffer;

				//Reset handshakeSAEA buffer to previous buffer (i.e. byte[4096] => comBuffer)
				handshakeSAEA.SetBuffer(frameInfo.Buffer, frameInfo.BufferOffset, frameInfo.BufferLength);

				if(handshakeSAEA.BytesTransferred == 0) {
					ts.TraceInformation("End of client stream");
					CloseConnection(handshakeSAEA, 1000, "");//TODO

				} else if(matches != 4) {
					ts.TraceInformation("Html header end not found, max header size exceeded");
					CloseConnection(handshakeSAEA, 1000, "");//TODO

				} else if(!ParseHtmlUpgradeHeader(frameInfo.Client, header, frameInfo.BufferPosition)) {
					ts.TraceInformation("Invalid html header");
					CloseConnection(handshakeSAEA, 1000, "");//TODO

				} else if(pos != frameInfo.BufferPosition) {
					ts.TraceInformation("Data after html header");
					CloseConnection(handshakeSAEA, 1000, "");//TODO

				} else {
					ts.TraceInformation("Generating WSAccept key");
					byte[] acceptKeyBin = Encoding.ASCII.GetBytes(frameInfo.Client.Key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
					lock(sha1HashObj) acceptKeyBin = sha1HashObj.ComputeHash(acceptKeyBin);

					ts.TraceInformation("Preparing response operation");
					var client = frameInfo.Client;
					frameInfo.Reset();
					frameInfo.Action = WSFrameInfoAction.HandshakeSend;
					frameInfo.Client = client;

					var resp = string.Format(ServerHandshakeFormat, Convert.ToBase64String(acceptKeyBin));
					var rspByteCount = Encoding.ASCII.GetBytes(resp, 0, resp.Length, handshakeSAEA.Buffer, handshakeSAEA.Offset);

					if(settings.UseTLS) {
						ts.TraceInformation("Encrypting response");
						rspByteCount = frameInfo.Client.SecureStream.EncryptInPlace(handshakeSAEA.Buffer, handshakeSAEA.Offset, rspByteCount);
					}

					frameInfo.GetSAEABuffer(handshakeSAEA);
					frameInfo.HeaderLength = rspByteCount;
					handshakeSAEA.SetBuffer(frameInfo.BufferOffset, rspByteCount);

					ts.TraceInformation("Sending response");
					if(!frameInfo.Client.Socket.SendAsync(handshakeSAEA)) {
						ts.TraceInformation("SendAsync finished synchronously");
						ProcessSend(handshakeSAEA);
					}

					ts.TraceInformation("Preparing receive operation");
					if(!frameInfoPool.TryDequeue(out frameInfo)) frameInfo = new WSFrameInfo();
					frameInfo.Action = WSFrameInfoAction.Receive;
					frameInfo.Client = client;

					SocketAsyncEventArgs receiveSAEA;
					if(transferSAEAPool.TryDequeue(out receiveSAEA)) {
						ts.TraceInformation("Starting receive operation");
						receiveSAEA.UserToken = frameInfo;
						frameInfo.GetSAEABuffer(receiveSAEA);
						StartReceive(receiveSAEA);

					} else {
						ts.TraceInformation("Enqueuing receive operation");
						pendingOperations.Enqueue(frameInfo);
					}
				}
			}
		}
		private bool ParseHtmlUpgradeHeader(WSClientInfo client, byte[] header, int headerLength) {
			if(headerLength < 100) {
				ts.TraceInformation("Header too small");
				return false; //TODO: Determine smallest header
			}

			//var clientHeader = Encoding.ASCII.GetString(header);

			//TODO: Proper header parsing
			if(header[0] != 'G' || header[1] != 'E' || header[2] != 'T' || header[3] != ' ') return false;

			int pos = 4, lastPos;

			while(pos < headerLength && header[pos] != ' ') pos++;
			if(header[pos] != ' ') return false;
			client.Uri = Encoding.ASCII.GetString(header, 4, pos - 4);

			pos++;
			lastPos = pos;
			while(pos < headerLength - 1 && (header[pos] != '\r' || header[++pos] != '\n')) pos++;
			if(header[pos] != '\n') return false;
			client.HttpVersion = Encoding.ASCII.GetString(header, lastPos, pos - lastPos - 1);

			var isValid = true;

			pos++;
			while(true) {
				lastPos = pos;
				var colonPos = -1;
				while(pos < headerLength - 1 && (header[pos] != '\r' || header[++pos] != '\n')) {
					if(colonPos == -1 && header[pos] == ':') colonPos = pos;
					pos++;
				}
				if(header[pos] != '\n' || lastPos >= pos - 2) break;
				pos--;

				if(colonPos == -1) colonPos = pos;
				var fieldName = Encoding.ASCII.GetString(header, lastPos, colonPos - lastPos).ToLower();
				switch(fieldName) {
					case "host": client.Host = Encoding.ASCII.GetString(header, colonPos + 1, pos - colonPos - 1).Trim(); break;
					case "user-agent": client.UserAgent = Encoding.ASCII.GetString(header, colonPos + 1, pos - colonPos - 1).Trim(); break;
					case "upgrade": isValid &= Encoding.ASCII.GetString(header, colonPos + 1, pos - colonPos - 1).ToLower().IndexOf("websocket") != -1; break;
					case "connection": isValid &= Encoding.ASCII.GetString(header, colonPos + 1, pos - colonPos - 1).ToLower().IndexOf("upgrade") != -1; break;
					case "sec-websocket-key": client.Key = Encoding.ASCII.GetString(header, colonPos + 1, pos - colonPos - 1).Trim(); break;
					case "origin": client.Origin = Encoding.ASCII.GetString(header, colonPos + 1, pos - colonPos - 1).Trim(); break;
					case "sec-websocket-version": client.WebSocketVersion = Encoding.ASCII.GetString(header, colonPos + 1, pos - colonPos - 1).Trim(); break;
					case "sec-websocket-protocol": client.Protocol = Encoding.ASCII.GetString(header, colonPos + 1, pos - colonPos - 1).Trim(); break;
					case "sec-websocket-extensions": client.Extensions = Encoding.ASCII.GetString(header, colonPos + 1, pos - colonPos - 1).Trim(); break;
					default:
						break;
				}

				pos += 2;
			}
			//TODO: Checks

			return true;
		}

		private void OnIOCompleted(object sender, SocketAsyncEventArgs saea) {
			var frameInfo = (WSFrameInfo)saea.UserToken;

			using(ts.CreateActivity("WebSocketClient OnIOCompleted", true, frameInfo.Client.Id)) {
				if(saea.SocketError == SocketError.Success) {
					switch(saea.LastOperation) {
						case SocketAsyncOperation.Receive: ProcessReceive(saea); break;
						case SocketAsyncOperation.Send: ProcessSend(saea); break;
						default: throw new ArgumentException("The last operation completed on the socket was not a receive or send");
					}

				} else if(saea.SocketError == SocketError.TimedOut) {
					ts.TraceInformation("Operation timed out, re-enqueuing it");
					Collect(saea, true);

				} else {
					ts.TraceInformation("Encountered unhandled error");
					CloseConnection(saea, 1000, "");//TODO
				}

				ProcessPendingOperations();
			}
		}

		private void StartReceive(SocketAsyncEventArgs receiveSAEA) {
			var frameInfo = (WSFrameInfo)receiveSAEA.UserToken;
			using(ts.CreateActivity("WebSocketClient StartReceive", true, frameInfo.Client.Id)) {
				receiveSAEA.SetBuffer(frameInfo.BufferOffset + frameInfo.BufferPosition, frameInfo.BufferLength - frameInfo.BufferPosition);

				bool finishedSync;
				try {
					finishedSync = !frameInfo.Client.Socket.ReceiveAsync(receiveSAEA);

				} catch(ObjectDisposedException ex) {
					//TODO: logging
					finishedSync = false;
					receiveSAEA.SetBuffer(frameInfo.BufferOffset, frameInfo.BufferLength);
					Collect(receiveSAEA, false);
				}

				if(finishedSync) {
					ts.TraceInformation("ReceiveAsync finished synchronously");
					ProcessReceive(receiveSAEA);
				}
			}
		}
		private void ProcessReceive(SocketAsyncEventArgs receiveSAEA) {
			var frameInfo = (WSFrameInfo)receiveSAEA.UserToken;

			using(ts.CreateActivity("WebSocketClient ProcessReceive", true, frameInfo.Client.Id)) {
				if(receiveSAEA.SocketError != SocketError.Success) {
					ts.TraceInformation("Receive operation didn't complete successfully");
					CloseConnection(receiveSAEA, 1000, "");//TODO

				} else if(receiveSAEA.BytesTransferred == 0) {
					ts.TraceInformation("End of client stream");
					CloseConnection(receiveSAEA, 1000, "");//TODO

				} else {
					Interlocked.Add(ref bytesReceived, receiveSAEA.BytesTransferred);

					int bytesRead;
					if(settings.UseTLS) {
						ts.TraceInformation("Decrypting data");
						bytesRead = frameInfo.Client.SecureStream.DecryptInPlace(receiveSAEA.Buffer, receiveSAEA.Offset, receiveSAEA.BytesTransferred);
					} else {
						bytesRead = receiveSAEA.BytesTransferred;
					}

					ts.TraceInformation("BytesTransferred={0}, bytesRead={1}", receiveSAEA.BytesTransferred, bytesRead);
					frameInfo.BufferPosition += bytesRead;

					int statusCode;
					string statusMessage;
					var immediateReceive = ProcessReceiveSub(frameInfo, out statusCode, out statusMessage);

					if(statusCode != -1) {
						ts.TraceInformation("Closing connection");
						CloseConnection(receiveSAEA, 1000, "");//TODO


						//frameInfo.Buffer[frameInfo.BufferOffset + 1] &= 127;
						//Buffer.BlockCopy(
						//	frameInfo.Buffer, frameInfo.BufferOffset + frameInfo.HeaderLength,
						//	frameInfo.Buffer, frameInfo.BufferOffset + frameInfo.HeaderLength - 4,
						//	frameInfo.BufferPosition - frameInfo.HeaderLength
						//);
						//frameInfo.BufferPosition -= 4;
						//CloseConnection(receiveSAEA);

					} else if(immediateReceive || frameInfoPool.Count > 1) { //TODO?: A more threadsafe way needed?
						//Skip pendingOperations since there enough FrameInfo available in frameInfoPool
						//Or we haven't either filled the saea buffer or the ws message has not been fully read
						ts.TraceInformation("Starting receive operation");
						StartReceive(receiveSAEA);

					} else {
						ts.TraceInformation("Enqueuing receive operation and cleaning up old operation");
						Collect(receiveSAEA, true);
					}
				}

				ProcessPendingOperations();
			}
		}
		private bool ProcessReceiveSub(WSFrameInfo frameInfo, out int statusCode, out string statusMessage) {
			statusCode = -1;
			statusMessage = null;

			//Caution: Header.Length must be <= frameInfo.BufferLength
			if(!frameInfo.HeaderProcessed && !ParseFrameHeader(frameInfo, out statusCode, out statusMessage)) {
				return true;

			} else if(frameInfo.UnmaskedBytes == 0) {
				//Header is still in buffer
				frameInfo.DataLength = (int)Math.Min(frameInfo.PayloadLength, frameInfo.BufferPosition - frameInfo.HeaderLength);

			} else {
				//HandleData was called at least once, so it is safe to assume the header is already processed and not in the buffer anymore
				frameInfo.DataLength = (int)Math.Min(frameInfo.PayloadLength - frameInfo.UnmaskedBytes, frameInfo.BufferPosition);
			}

			ts.TraceInformation(frameInfo.ToString());

			int nextFrameLength;
			if(frameInfo.UnmaskedBytes == 0) {
				nextFrameLength = (int)Math.Max(0, frameInfo.BufferPosition - frameInfo.TotalLength);
			} else {
				nextFrameLength = frameInfo.BufferPosition - frameInfo.DataLength;
			}

			ts.TraceInformation("Handling payload(nextFrameLength={0})", nextFrameLength);


			frameInfo.BufferPosition -= nextFrameLength;

			//if(frameInfo.UnmaskedBytes + frameInfo.BufferPosition == frameInfo.TotalLength) {
			if(frameInfo.UnmaskedBytes + frameInfo.DataLength == frameInfo.PayloadLength) {
				//Case 2 & 3: (One ws-frame received) or (At least one ws-frame with possibly another partial ws-frame as last received)
				ts.TraceInformation("Got full ws message, present to upper layer");
				HandleData(frameInfo, out statusCode, out statusMessage);
				if(statusCode != -1) return false;

				var client = frameInfo.Client;
				var opCode = frameInfo.OpCode;
				var buffer = frameInfo.Buffer;
				var bufferOffset = frameInfo.BufferOffset;
				var bufferPosition = frameInfo.BufferPosition;
				var bufferLength = frameInfo.BufferLength;
				var payloadHandler = frameInfo.PayloadHandler;
				var tag = frameInfo.Tag;

				frameInfo.Reset();
				frameInfo.Action = WSFrameInfoAction.Receive;
				frameInfo.OpCode = opCode; //Needed?
				frameInfo.Client = client;
				frameInfo.Buffer = buffer;
				frameInfo.BufferOffset = bufferOffset;
				frameInfo.BufferLength = bufferLength;
				frameInfo.PayloadHandler = payloadHandler;
				frameInfo.Tag = tag;

				if(nextFrameLength > 0) {
					//Case 3: At least one ws-frame with possibly another partial ws-frame as last received

					//Move next frame (and following bytes) to start of buffer
					Buffer.BlockCopy( //TODO?: Only do a Buffer.BlockCopy when necessary (so case 4 is also possible)
						frameInfo.Buffer, frameInfo.BufferOffset + bufferPosition,
						frameInfo.Buffer, frameInfo.BufferOffset, nextFrameLength
					);

					//Update bytes transferred to remaining unprocessed bytes
					frameInfo.BufferPosition = nextFrameLength;

					//Process next frame
					ts.TraceInformation("Processing next frame in same data segment");

					return ProcessReceiveSub(frameInfo, out statusCode, out statusMessage);
				}

			} else if(frameInfo.BufferPosition == frameInfo.BufferLength) {
				//Case 4: SAEA buffer full -> Transfer data to parent layer so the buffer can be reused
				ts.TraceInformation("Got full buffer, present to upper layer");
				HandleData(frameInfo, out statusCode, out statusMessage);
				frameInfo.DataOffset = frameInfo.BufferOffset;
				//frameInfo.HeaderLength = 0; //TODO?: Don't manipulate
				frameInfo.BufferPosition = 0;
				if(statusCode != -1) return false;

			} else {
				//Need next receive operation immediately
				ts.TraceInformation("Need more data");
				return true;
			}
			//Next receive operation may be queued
			ts.TraceInformation("Finished");
			return false;
		}
		private bool ParseFrameHeader(WSFrameInfo frameInfo, out int statusCode, out string statusMessage) {
			statusCode = -1;
			statusMessage = null;

			ts.TraceInformation("Processing header");

			//First length indicator is at byte 2
			var canProcessHeader = frameInfo.BufferPosition > 1;

			var headerLength = 0;
			var lengthIndicator = canProcessHeader ? frameInfo.Buffer[frameInfo.BufferOffset + 1] : 0;
			var isMasked = (lengthIndicator & 128) != 0;
			lengthIndicator &= 127;

			//var payloadLengthFieldLength = (lengthIndicator == 126 ? 3 : (lengthIndicator == 127 ? 9 : 1));
			headerLength = GetSizeFieldLengthPlusOne(lengthIndicator);

			//var maskLength = 0;
			if(isMasked) {
				//Warning: this way throw if buffer is too small
				frameInfo.Mask = BitConverter.ToUInt32(frameInfo.Buffer, frameInfo.BufferOffset + headerLength);
				frameInfo.Mask |= frameInfo.Mask << 32;

				headerLength += 4;
			}

			canProcessHeader &= frameInfo.BufferPosition >= headerLength;

			//Case 1: Not enough header data
			if(!canProcessHeader) {
				ts.TraceInformation("Not enough header data");
				return false;
			}


			frameInfo.IsFinal = (frameInfo.Buffer[frameInfo.BufferOffset] & 128) != 0;
			frameInfo.OpCode = (WSFrameInfoOpCode)(frameInfo.Buffer[frameInfo.BufferOffset] & 15);
			frameInfo.Version = (WSFrameInfoVersion)((frameInfo.Buffer[frameInfo.BufferOffset] >> 4) & 7);
			frameInfo.HeaderLength = headerLength;
			frameInfo.IsMasked = isMasked;

			switch(lengthIndicator) {
				//RFC: https://tools.ietf.org/html/rfc6455#section-5.2 "Payload length"
				case 126:
					frameInfo.PayloadLength =
						(frameInfo.Buffer[frameInfo.BufferOffset + 2] << 8) |
						 frameInfo.Buffer[frameInfo.BufferOffset + 3];
					break;

				case 127:
					frameInfo.PayloadLength =
						((long)frameInfo.Buffer[frameInfo.BufferOffset + 2] << 56) |
						((long)frameInfo.Buffer[frameInfo.BufferOffset + 3] << 48) |
						((long)frameInfo.Buffer[frameInfo.BufferOffset + 4] << 40) |
						((long)frameInfo.Buffer[frameInfo.BufferOffset + 5] << 32) |
						((long)frameInfo.Buffer[frameInfo.BufferOffset + 6] << 24) |
						((long)frameInfo.Buffer[frameInfo.BufferOffset + 7] << 16) |
						((long)frameInfo.Buffer[frameInfo.BufferOffset + 8] << 8) |
						 (long)frameInfo.Buffer[frameInfo.BufferOffset + 9];
					break;

				default: frameInfo.PayloadLength = lengthIndicator; break;
			}

			frameInfo.HeaderProcessed = true;

			//TODO: Close Connection if isMasked is false https://tools.ietf.org/html/rfc6455#section-5.1
			//TODO: Close Connection if control frame but isFinal not set
			//TODO: Close Connection if continuation chain started with a continuation frame

			frameInfo.PayloadHandler = frameInfo.Client.DefaultPayloadHandler;
			var payloadHandlerRequest = PayloadHandlerRequest;
			if(frameInfo.OpCode != WSFrameInfoOpCode.Continuation && payloadHandlerRequest != null) {
				frameInfo.PayloadHandler = payloadHandlerRequest(frameInfo);
			}

			frameInfo.DataOffset = frameInfo.BufferOffset + frameInfo.HeaderLength;
			frameInfo.DataLength = (int)Math.Min(frameInfo.PayloadLength, frameInfo.BufferPosition - frameInfo.HeaderLength);

			return true;
		}
		private void HandleData(WSFrameInfo frameInfo, out int statusCode, out string statusMessage) {
			statusCode = -1;
			statusMessage = null;

			var asg1 = Encoding.UTF8.GetString(frameInfo.Buffer, frameInfo.DataOffset, frameInfo.DataLength);

			UnmaskData(frameInfo);

			var asg2 = Encoding.UTF8.GetString(frameInfo.Buffer, frameInfo.DataOffset, frameInfo.DataLength);

			frameInfo.UnmaskedBytes += frameInfo.DataLength;

			if((int)frameInfo.OpCode < 3) { //True iif non control frame https://tools.ietf.org/html/rfc6455#section-5.2
				if(frameInfo.PayloadHandler != null) {
					try {
						frameInfo.PayloadHandler.Handle(frameInfo);

					} catch(Exception) {
						//TODO: Give upper layer chance to handle exception
						if(false) throw;
					}
				}

			} else if((int)frameInfo.OpCode < 8) {
				statusCode = 1000; //TODO
				statusMessage = "Unknown non-control frame";

			} else if(frameInfo.OpCode == WSFrameInfoOpCode.Close) {
				statusCode = 1001;
				statusMessage = "Connection closed by client";

			} else if(frameInfo.OpCode == WSFrameInfoOpCode.Ping) {
				//TODO: Raise Ping/Pong Event 
				Send(
					frameInfo.Client, pingPongGenerator,
					pingPongGenerator.AddOrUpdate(false, frameInfo.Buffer, frameInfo.DataOffset, frameInfo.DataLength)
				);

			} else if(frameInfo.OpCode == WSFrameInfoOpCode.Pong) {
				//TODO: Raise Ping/Pong Event 

			} else {
				statusCode = 1000; //TODO
				statusMessage = "Unknown control frame";
			}

		}
		private static unsafe void UnmaskData(WSFrameInfo frameInfo) {
			int offset = frameInfo.DataOffset;
			int length = frameInfo.DataLength;
			int maskOffset = (int)(frameInfo.UnmaskedBytes & 3);

			var mask = frameInfo.Mask;
			fixed(byte* bPtr = frameInfo.Buffer) {
				int toShift = ((4 - maskOffset) & 3) << 3;
				mask = mask << toShift | mask >> (64 - toShift);

				byte* bPos = bPtr + offset;

				//Unmask data until we are at a long boundary
				int toBoundary = (8 - (int)((ulong)bPos & 7)) & 7;
				if(toBoundary > length) toBoundary = length;

				if((toBoundary & 4) != 0) {
					*(uint*)bPos ^= (uint)mask;
					bPos += 4;
					//No need to rotate mask
				}
				if((toBoundary & 2) != 0) {
					*(ushort*)bPos ^= (ushort)mask;
					bPos += 2;
					mask = mask << 16 | mask >> (64 - 16); //Rotate by 2 bytes
				}
				if((toBoundary & 1) != 0) {
					*bPos ^= (byte)mask;
					bPos++;
					mask = mask << 24 | mask >> (64 - 24); //Rotate by 1 byte
				}

				//Unmask longs
				ulong* pos = (ulong*)bPos;
				ulong* posLimit = pos + ((length - toBoundary) >> 3);
				while(pos != posLimit) *(pos++) ^= mask;

				//Unmask remaining data
				int bytesLeft = (int)((bPtr + offset + length) - (byte*)pos);
				bPos = (byte*)pos;

				if((bytesLeft & 4) != 0) {
					*(uint*)bPos ^= (uint)mask;
					bPos += 4;
				}
				if((bytesLeft & 2) != 0) {
					*(ushort*)bPos ^= (ushort)mask;
					bPos += 2;
					mask = mask << 16 | mask >> (64 - 16);
				}
				if((bytesLeft & 1) != 0) {
					*bPos ^= (byte)mask;
				}
			}
		}

		internal void Send(WSClientInfo client, ISegmentGenerator generator, object tag) {
			WSFrameInfo frameInfo;
			if(!frameInfoPool.TryDequeue(out frameInfo)) frameInfo = new WSFrameInfo();
			frameInfo.Action = WSFrameInfoAction.Send;
			frameInfo.Client = client;
			frameInfo.SegmentGenerator = generator;
			frameInfo.Tag = tag;

			frameInfo.OpCode = generator.Type(frameInfo);

			//TODO?: Enqueue only to guaranty send order
			SocketAsyncEventArgs sendSAEA;
			if(transferSAEAPool.TryDequeue(out sendSAEA)) {
				sendSAEA.UserToken = frameInfo;
				frameInfo.GetSAEABuffer(sendSAEA);

				StartSend(sendSAEA);

			} else {
				pendingOperations.Enqueue(frameInfo);
			}
		}
		private void StartSend(SocketAsyncEventArgs sendSAEA) {
			var frameInfo = (WSFrameInfo)sendSAEA.UserToken;

			if(frameInfo.MessagePayloadLength != 0) {
				//A continuation of a previous partial frame
				//Partial frames can only be sent when frameInfo.MessagePayloadLength != 0 otherwise only complete packets will be sent through this method
				//It is safe to assume that the frame header is already sent, so we can solely concentrate on sending data
				StartSendContinuePartialFrame(frameInfo);

			} else {
				//A new frame needs to be sent
				StartSendNewFrame(frameInfo);
				frameInfo.OpCode = WSFrameInfoOpCode.Continuation;
			}

			sendSAEA.SetBuffer(frameInfo.DataOffset, frameInfo.DataLength);

			bool finishedSync;
			try {
				finishedSync = !frameInfo.Client.Socket.SendAsync(sendSAEA);

			} catch(ObjectDisposedException ex) {
				//TODO: logging
				finishedSync = false;
				sendSAEA.SetBuffer(frameInfo.BufferOffset, frameInfo.BufferLength);
				Collect(sendSAEA, false);
			}

			if(finishedSync) {
				ProcessSend(sendSAEA);
			}
		}
		private void StartSendNewFrame(WSFrameInfo frameInfo) {
			frameInfo.MessagePayloadLength = frameInfo.SegmentGenerator.Length(frameInfo);
			if(frameInfo.MessagePayloadLength < 0) {
				throw new InvalidOperationException("Message length must be positive");
			}

			int maxSegmentPayloadLength;
			if(frameInfo.MessagePayloadLength != 0) {
				frameInfo.HeaderLength = GetSizeFieldLengthPlusOne(frameInfo.MessagePayloadLength);
				frameInfo.PayloadLength = frameInfo.MessagePayloadLength - frameInfo.MessagePayloadBytesTransferred;

				maxSegmentPayloadLength = (int)Math.Min(
					frameInfo.PayloadLength,
					(MaxSendFrameLength == 0 ? Int32.MaxValue : MaxSendFrameLength) - frameInfo.HeaderLength
				);

			} else {
				//Assume worst case
				frameInfo.HeaderLength = GetSizeFieldLengthPlusOne(frameInfo.BufferLength - 2);
				frameInfo.PayloadLength = 0;

				maxSegmentPayloadLength = MaxSendFrameLength == 0 ?
					Int32.MaxValue : MaxSendFrameLength - frameInfo.HeaderLength;
			}

			if(maxSegmentPayloadLength > frameInfo.BufferLength) {
				maxSegmentPayloadLength = frameInfo.BufferLength - frameInfo.HeaderLength;
			}

			bool completed;
			frameInfo.DataLength = maxSegmentPayloadLength /*- frameInfo.HeaderLength*/;
			frameInfo.DataOffset = frameInfo.BufferOffset + frameInfo.HeaderLength;
			var bytesRead = frameInfo.SegmentGenerator.Read(frameInfo, out completed);

			//var sdgf = frameInfo.Buffer.Skip(frameInfo.DataOffset).Take(bytesRead).All(b => b == 'a');

			var messageBytesRead = frameInfo.MessagePayloadBytesTransferred + bytesRead;

			var isLengthConsistent = frameInfo.MessagePayloadLength == 0 || (
				(!completed || frameInfo.MessagePayloadLength == messageBytesRead) &&
				(completed || frameInfo.MessagePayloadLength > messageBytesRead)
			);

			if(!isLengthConsistent) throw new InvalidOperationException("Message length inconsistent");


			if(frameInfo.PayloadLength == 0) {
				frameInfo.HeaderLength = GetSizeFieldLengthPlusOne(bytesRead);
				frameInfo.PayloadLength = bytesRead;
			}

			frameInfo.DataOffset -= frameInfo.HeaderLength;
			frameInfo.DataLength = frameInfo.HeaderLength + bytesRead;

			if(completed) frameInfo.MessagePayloadLength = messageBytesRead;

			frameInfo.Buffer[frameInfo.DataOffset] = (byte)frameInfo.OpCode;

			if(messageBytesRead == frameInfo.MessagePayloadLength) {
				frameInfo.IsFinal = true;
				frameInfo.Buffer[frameInfo.DataOffset] |= 128;
			}

			if(frameInfo.HeaderLength == 2) {
				frameInfo.Buffer[frameInfo.DataOffset + 1] = (byte)frameInfo.PayloadLength;

			} else if(frameInfo.HeaderLength == 4) {
				frameInfo.Buffer[frameInfo.DataOffset + 1] = 126;
				frameInfo.Buffer[frameInfo.DataOffset + 2] = (byte)(frameInfo.PayloadLength >> 08);
				frameInfo.Buffer[frameInfo.DataOffset + 3] = (byte)(frameInfo.PayloadLength >> 00);

			} else if(frameInfo.HeaderLength == 10) {
				frameInfo.Buffer[frameInfo.DataOffset + 1] = 127;

				//Only using int for maxSegmentPayloadLength so shifting does "x % 32" for the second operand
				frameInfo.Buffer[frameInfo.DataOffset + 2] = 0; //(byte)(frameInfo.PayloadLength >> 56);
				frameInfo.Buffer[frameInfo.DataOffset + 3] = 0; //(byte)(frameInfo.PayloadLength >> 48);
				frameInfo.Buffer[frameInfo.DataOffset + 4] = 0; //(byte)(frameInfo.PayloadLength >> 40);
				frameInfo.Buffer[frameInfo.DataOffset + 5] = 0; //(byte)(frameInfo.PayloadLength >> 32);
				frameInfo.Buffer[frameInfo.DataOffset + 6] = (byte)(frameInfo.PayloadLength >> 24);
				frameInfo.Buffer[frameInfo.DataOffset + 7] = (byte)(frameInfo.PayloadLength >> 16);
				frameInfo.Buffer[frameInfo.DataOffset + 8] = (byte)(frameInfo.PayloadLength >> 08);
				frameInfo.Buffer[frameInfo.DataOffset + 9] = (byte)(frameInfo.PayloadLength >> 00);

			} else {
				throw new InvalidOperationException("Invalid size class");
			}

		}
		private void StartSendContinuePartialFrame(WSFrameInfo frameInfo) {
			frameInfo.DataOffset = frameInfo.BufferOffset;
			frameInfo.DataLength = (int)Math.Min(
				Math.Min(MaxSendFrameLength, frameInfo.BufferLength),
				frameInfo.MessagePayloadLength - frameInfo.MessagePayloadBytesTransferred
			);

			bool completed;
			var bytesRead = frameInfo.SegmentGenerator.Read(frameInfo, out completed);

			var messageBytesRead = frameInfo.MessagePayloadBytesTransferred + bytesRead;

			//TODO?: Allow zero length reads
			var isConsistent =
				(!completed || messageBytesRead == frameInfo.MessagePayloadLength) &&
				(completed || messageBytesRead < frameInfo.MessagePayloadLength);

			if(isConsistent) throw new InvalidOperationException("Message length returned by generator is inconsistent");

			frameInfo.DataLength = bytesRead;
		}
		private void ProcessSend(SocketAsyncEventArgs sendSAEA) {
			var frameInfo = (WSFrameInfo)sendSAEA.UserToken;
			sendSAEA.SetBuffer(frameInfo.BufferOffset, frameInfo.BufferLength);

			if(sendSAEA.SocketError != SocketError.Success || frameInfo.Action == WSFrameInfoAction.Close) {
				if(frameInfo.Client.Socket.Connected) {
					CloseConnection(sendSAEA, 1000, "Error while receiving from client"); //TODO
				} else {
					if(Interlocked.Exchange(ref frameInfo.Client.ConnectedFlag, 0) == 1) {
						CloseClientSocket(frameInfo.Client.Socket);
					}
					Collect(sendSAEA, false);
				}

			} else if(frameInfo.Action == WSFrameInfoAction.HandshakeSend) {
				Collect(sendSAEA, false);

			} else if(frameInfo.Action == WSFrameInfoAction.Send) {
				frameInfo.OpCode = WSFrameInfoOpCode.Continuation;
				frameInfo.MessagePayloadBytesTransferred += frameInfo.PayloadLength;

				var isMessageSent = frameInfo.MessagePayloadBytesTransferred == frameInfo.MessagePayloadLength;
				if(!isMessageSent && frameInfoPool.Count > 1) { //TODO?: A more threadsafe way needed?
					ts.TraceInformation("Starting send operation");
					StartSend(sendSAEA);

				} else {
					ts.TraceInformation("Enqueuing send operation and cleaning up old operation");
					Collect(sendSAEA, !isMessageSent);
				}
			}

			ProcessPendingOperations();
		}

		private void Collect(SocketAsyncEventArgs saea, bool enqueueOperation) {
			var frameInfo = (WSFrameInfo)saea.UserToken;
			saea.UserToken = null;

			if(enqueueOperation) {
				pendingOperations.Enqueue(frameInfo);

			} else {
				frameInfo.Reset();
				frameInfoPool.Enqueue(frameInfo);
			}

			if(saea.Count != settings.TransferBufferSize || (saea.Offset % settings.TransferBufferSize) != 0) {
				var bla = 0;
			}

			saea.SetBuffer(saea.Offset, settings.TransferBufferSize);
			transferSAEAPool.Enqueue(saea);
		}

		private void CloseConnection(SocketAsyncEventArgs closeSAEA, int closeCode, string closeReason) {
			var frameInfo = (WSFrameInfo)closeSAEA.UserToken;

			frameInfo.Buffer[frameInfo.BufferOffset] = 128 | (byte)WSFrameInfoOpCode.Close;

			var msgByteLength = Encoding.UTF8.GetByteCount(closeReason);
			if(msgByteLength < 126) {
				frameInfo.Buffer[frameInfo.BufferOffset + 1] = (byte)msgByteLength;
				frameInfo.Buffer[frameInfo.BufferOffset + 2] = (byte)(closeCode >> 08);
				frameInfo.Buffer[frameInfo.BufferOffset + 3] = (byte)(closeCode >> 00);

				frameInfo.BufferPosition = 4;

			} else if(msgByteLength < Int16.MaxValue) {
				frameInfo.Buffer[frameInfo.BufferOffset + 1] = 126;
				frameInfo.Buffer[frameInfo.BufferOffset + 2] = (byte)(msgByteLength >> 08);
				frameInfo.Buffer[frameInfo.BufferOffset + 3] = (byte)(msgByteLength >> 00);
				frameInfo.Buffer[frameInfo.BufferOffset + 4] = (byte)(closeCode >> 08);
				frameInfo.Buffer[frameInfo.BufferOffset + 5] = (byte)(closeCode >> 00);
				frameInfo.BufferPosition = 6;

			} else {
				throw new InvalidOperationException("Close reason too long");
			}

			frameInfo.BufferPosition = frameInfo.HeaderLength + msgByteLength;
			Encoding.UTF8.GetBytes(closeReason, 0, closeReason.Length, frameInfo.Buffer, frameInfo.BufferOffset + frameInfo.HeaderLength);


			CloseConnection(closeSAEA);
		}
		private void CloseConnection(SocketAsyncEventArgs closeSAEA) {
			var frameInfo = (WSFrameInfo)closeSAEA.UserToken;

			frameInfo.Action = WSFrameInfoAction.Close;
			closeSAEA.SetBuffer(frameInfo.BufferOffset, frameInfo.BufferPosition);

			bool finishedSync;
			try {
				finishedSync = !frameInfo.Client.Socket.SendAsync(closeSAEA);

			} catch(ObjectDisposedException ex) {
				//TODO: logging
				finishedSync = false;

				if(Interlocked.Exchange(ref frameInfo.Client.ConnectedFlag, 0) == 1) {
					CloseClientSocket(frameInfo.Client.Socket);
				}

				Collect(closeSAEA, false);
			}

			if(finishedSync) {
				ProcessSend(closeSAEA);
			}

		}

		private void CloseClientSocket(Socket socket) {
			try {
				socket.Shutdown(SocketShutdown.Both);
			} catch(Exception) { }

			socket.Close();
			this.connectionLimiter.Release();
		}

		private void ProcessPendingOperations() {
			if(pendingOperations.Count == 0 || transferSAEAPool.Count == 0) return; //TODO: Verify if safe

			while(true) {
				WSFrameInfo frameInfo;
				SocketAsyncEventArgs saea;
				if(transferSAEAPool.TryDequeue(out saea) && pendingOperations.TryDequeue(out frameInfo)) {
					frameInfo.GetSAEABuffer(saea);
					saea.UserToken = frameInfo;

					switch(frameInfo.Action) {
						case WSFrameInfoAction.HandshakeReceive: InitHandshake(saea); break;
						case WSFrameInfoAction.Receive: StartReceive(saea); break;
						case WSFrameInfoAction.Send: StartSend(saea); break;
						case WSFrameInfoAction.HandshakeSend: StartSend(saea); break;
						default: throw new InvalidOperationException("Unknown WSFrameInfoAction");
					}

				} else {
					if(saea != null) transferSAEAPool.Enqueue(saea);
					break;
				}
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		private static int GetSizeFieldLengthPlusOne(long length) { return length < 126 ? 2 : (length == 126 ? 4 : 10); }

		private const string ServerHandshakeFormat =
			"HTTP/1.1 101 Switching Protocols\r\n" +
			"Upgrade: WebSocket\r\n" +
			"Connection: Upgrade\r\n" +
			"Sec-WebSocket-Accept: {0}\r\n\r\n";
	}
}
