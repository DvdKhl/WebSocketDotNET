using DvdKhl;
using System;
using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using WebSocket.PayloadHandlers;
using WebSocket.SegmentGenerators;
using WebSocket.Streams;

namespace WebSocket {
	public delegate IPayloadHandler PayloadHandlerRequestHandler(WSFrameInfo frameInfo);

	public class WebSocketServer {
		private static Guid operationsActivityId = Guid.NewGuid();
		protected static TraceSourceEx ts = new TraceSourceEx("WebSocket.WebSocketServer", System.Diagnostics.SourceLevels.All);

		public event PayloadHandlerRequestHandler PayloadHandlerRequest;

		private byte[] comBuffer;
		private Socket listenSocket;

		private BlockingCollection<SocketAsyncEventArgs> acceptSAEAPool;
		private ConcurrentQueue<SocketAsyncEventArgs> transferSAEAPool;
		private ConcurrentQueue<WSFrameInfo> frameInfoPool;
		private ConcurrentQueue<WSFrameInfo> pendingOperations;

		private WebSocketServerSettings settings;

		private SemaphoreSlim connectionLimiter;
		private SHA1 sha1HashObj = SHA1.Create();

		public IPayloadHandler DefaultPayloadHandler { get; set; }
		public int MaxFramePayloadLength { get; set; }

		public WebSocketServer(WebSocketServerSettings settings) {
			using(ts.CreateActivity("WebSocketServer Constructor")) {
				this.settings = settings;

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
				connectionLimiter.Wait();

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
					connectionLimiter.Release();

					StartAccept();
					return;
				}

				ts.TraceInformation("Preparing Handshake operation");
				WSFrameInfo frameInfo;
				if(!frameInfoPool.TryDequeue(out frameInfo)) {
					ts.TraceInformation("Created WSFrameInfo");
					frameInfo = new WSFrameInfo();
				}

				frameInfo.Action = WSFrameInfoAction.Handshake;

				frameInfo.Client = new WSClientInfo {
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

					frameInfo.Client.Socket.ReceiveTimeout = 10000;
					frameInfo.Client.Socket.SendTimeout = 10000;

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
		private void OnHandshake(object sender, SocketAsyncEventArgs saea) { ProcessHandshake(saea); }
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
					CloseClientSocketAndContinue(handshakeSAEA);

				} else if(matches != 4) {
					ts.TraceInformation("Html header end not found, max header size exceeded");
					CloseClientSocketAndContinue(handshakeSAEA);

				} else if(!ParseHtmlUpgradeHeader(frameInfo.Client, header, frameInfo.BufferPosition)) {
					ts.TraceInformation("Invalid html header");
					CloseClientSocketAndContinue(handshakeSAEA);

				} else if(pos != frameInfo.BufferPosition) {
					ts.TraceInformation("Data after html header");
					CloseClientSocketAndContinue(handshakeSAEA);

				} else {
					ts.TraceInformation("Generating WSAccept key");
					byte[] acceptKeyBin = Encoding.ASCII.GetBytes(frameInfo.Client.Key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
					lock(sha1HashObj) acceptKeyBin = sha1HashObj.ComputeHash(acceptKeyBin);

					ts.TraceInformation("Preparing response operation");
					var client = frameInfo.Client;
					frameInfo.Reset();
					frameInfo.Action = WSFrameInfoAction.Send;
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
					FinishedOperationAndContinue(saea);

				} else {
					ts.TraceInformation("Encountered unhandled error");
					CloseClientSocketAndContinue(saea);
				}
			}
		}

		private void StartReceive(SocketAsyncEventArgs receiveSAEA) {
			var frameInfo = (WSFrameInfo)receiveSAEA.UserToken;
			using(ts.CreateActivity("WebSocketClient StartReceive", true, frameInfo.Client.Id)) {
				receiveSAEA.SetBuffer(frameInfo.BufferOffset + frameInfo.BufferPosition, frameInfo.BufferLength - frameInfo.BufferPosition);

				if(!frameInfo.Client.Socket.ReceiveAsync(receiveSAEA)) {
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
					CloseClientSocketAndContinue(receiveSAEA);

				} else if(receiveSAEA.BytesTransferred == 0) {
					ts.TraceInformation("End of client stream");
					CloseClientSocketAndContinue(receiveSAEA);

				} else {
					int bytesRead;
					if(settings.UseTLS) {
						ts.TraceInformation("Decrypting data");
						bytesRead = frameInfo.Client.SecureStream.DecryptInPlace(receiveSAEA.Buffer, receiveSAEA.Offset, receiveSAEA.BytesTransferred);
					} else {
						bytesRead = receiveSAEA.BytesTransferred;
					}

					//if(frameInfo.BufferPosition != 0) {
					//	var sdgh = 0;
					//}

					frameInfo.BytesTransferred += bytesRead;
					frameInfo.BufferPosition += bytesRead;


					int statusCode;
					string statusMessage;
					var immediateReceive = ProcessReceiveSub(frameInfo, out statusCode, out statusMessage);

					if(statusCode != -1) {
						ts.TraceInformation("Closing connection");

						if(!frameInfoPool.TryDequeue(out frameInfo)) frameInfo = new WSFrameInfo();

						SocketAsyncEventArgs sendSAEA;
						if(transferSAEAPool.TryDequeue(out sendSAEA)) {
							sendSAEA.UserToken = frameInfo;
							frameInfo.GetSAEABuffer(sendSAEA);

							StartSend(sendSAEA);

						} else {
							pendingOperations.Enqueue(frameInfo);
						}


						frameInfo.Client.Socket.SendAsync(sendSAEA);

						CloseClientSocketAndContinue(receiveSAEA); //TODO: Proper close

					} else if(immediateReceive || frameInfoPool.Count > 1) { //TODO?: A more threadsafe way needed?
						//Skip pendingOperations since there enough FrameInfo available in frameInfoPool
						//Or we haven't either filled the saea buffer or the ws message has not been fully read
						ts.TraceInformation("Starting receive operation");
						StartReceive(receiveSAEA);

					} else {
						ts.TraceInformation("Enqueuing receive operation and cleaning up old operation");
						FinishedOperationAndContinue(receiveSAEA);
					}
				}
			}
		}
		private bool ProcessReceiveSub(WSFrameInfo frameInfo, out int statusCode, out string statusMessage) {
			statusCode = -1;
			statusMessage = null;

			//Caution: Header.Length must be <= frameInfo.BufferLength
			if(!frameInfo.HeaderProcessed && !ParseFrameHeader(frameInfo, out statusCode, out statusMessage)) {
				return true;
			} else {
				frameInfo.DataLength = (int)Math.Min(frameInfo.PayloadLength, frameInfo.BufferPosition);
			}

			ts.TraceInformation(frameInfo.ToString());

			ts.TraceInformation("Handling payload");
			var nextFrameLength = (int)Math.Max(0, frameInfo.BytesTransferred - frameInfo.TotalLength);

			frameInfo.BytesTransferred -= nextFrameLength;
			frameInfo.BufferPosition -= nextFrameLength;

			if(frameInfo.BytesTransferred == frameInfo.TotalLength) {
				//Case 2 & 3: (One ws-frame received) or (At least one ws-frame with possibly another partial ws-frame as last received)
				ts.TraceInformation("Got full ws message, present to upper layer");
				UnmaskAndHandleData(frameInfo, out statusCode, out statusMessage);
				if(statusCode != -1) return false;

				var client = frameInfo.Client;
				var opCode = frameInfo.OpCode;
				var buffer = frameInfo.Buffer;
				var bufferOffset = frameInfo.BufferOffset;
				var bufferPosition = frameInfo.BufferPosition;
				var bufferLength = frameInfo.BufferLength;
				var payloadHandler = frameInfo.PayloadHandler;

				frameInfo.Reset();
				frameInfo.Action = WSFrameInfoAction.Receive;
				frameInfo.OpCode = opCode;
				frameInfo.Client = client;
				frameInfo.Buffer = buffer;
				frameInfo.BufferOffset = bufferOffset;
				//frameInfo.BufferPosition = bufferPosition;
				frameInfo.BufferLength = bufferLength;
				frameInfo.PayloadHandler = payloadHandler;


				if(nextFrameLength > 0) {
					//Case 3: At least one ws-frame with possibly another partial ws-frame as last received

					//Move next frame (and following bytes) to start of buffer
					Buffer.BlockCopy( //TODO?: Only do a Buffer.BlockCopy when necessary (so case 4 is also possible)
						frameInfo.Buffer, frameInfo.BufferOffset + bufferPosition,
						frameInfo.Buffer, frameInfo.BufferOffset, nextFrameLength
					);


					//Update bytes transferred to remaining unprocessed bytes
					frameInfo.BytesTransferred = nextFrameLength;
					frameInfo.BufferPosition = nextFrameLength;

					//Process next frame
					ts.TraceInformation("Processing next frame in same data segment");


					return ProcessReceiveSub(frameInfo, out statusCode, out statusMessage);

					//return ProcessReceiveSub(frameInfo);
				}

			} else if(frameInfo.BufferPosition == frameInfo.BufferLength) {
				//Case 4: SAEA buffer full -> Transfer data to parent layer so the buffer can be reused
				ts.TraceInformation("Got full buffer, present to upper layer");
				UnmaskAndHandleData(frameInfo, out statusCode, out statusMessage);
				frameInfo.DataOffset = frameInfo.BufferOffset;
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

			var lengthIndicator = canProcessHeader ? frameInfo.Buffer[frameInfo.BufferOffset + 1] : 0;
			var isMasked = (lengthIndicator & 128) != 0;
			lengthIndicator &= 127;

			var payloadLengthFieldLength = (lengthIndicator == 126 ? 3 : (lengthIndicator == 127 ? 9 : 1));

			var maskLength = 0;
			if(isMasked) {
				frameInfo.Mask[0] = frameInfo.Buffer[frameInfo.BufferOffset + 1 + payloadLengthFieldLength + 0];
				frameInfo.Mask[1] = frameInfo.Buffer[frameInfo.BufferOffset + 1 + payloadLengthFieldLength + 1];
				frameInfo.Mask[2] = frameInfo.Buffer[frameInfo.BufferOffset + 1 + payloadLengthFieldLength + 2];
				frameInfo.Mask[3] = frameInfo.Buffer[frameInfo.BufferOffset + 1 + payloadLengthFieldLength + 3];
				maskLength = 4;
			}

			canProcessHeader &= frameInfo.BufferPosition >= 1 + payloadLengthFieldLength + maskLength;

			//Case 1: Not enough header data
			if(!canProcessHeader) {
				ts.TraceInformation("Not enough header data");
				return false;
			}


			frameInfo.IsFinal = (frameInfo.Buffer[frameInfo.BufferOffset] & 128) != 0;
			frameInfo.OpCode = (WSFrameInfoOpCode)(frameInfo.Buffer[frameInfo.BufferOffset] & 15);
			frameInfo.Version = (WSFrameInfoVersion)((frameInfo.Buffer[frameInfo.BufferOffset] >> 4) & 7);
			frameInfo.IsMasked = isMasked;
			frameInfo.HeaderLength = 1 + payloadLengthFieldLength + maskLength;

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
			//Console.WriteLine(frameInfo.PayloadLength + " " + frameInfo.BufferPosition + " " + frameInfo.DataLength);

			return true;
		}
		private static void UnmaskAndHandleData(WSFrameInfo frameInfo, out int statusCode, out string statusMessage) {
			statusCode = -1;
			statusMessage = null;

			int offset = frameInfo.DataOffset;
			int length = frameInfo.DataLength;
			long bytePos = frameInfo.ProcessedPayloadBytes;

			for(int i = 0; i < length; i++, bytePos++) frameInfo.Buffer[i + offset] ^= frameInfo.Mask[bytePos % 4];

			var allA = true;
			for(int i = 0; i < length; i++) allA &= frameInfo.Buffer[i + offset] == 'a';
			if(allA) ts.TraceInformation("Data is all a and length is: " + length);

			if((int)frameInfo.OpCode < 3) { //True iif non control frame https://tools.ietf.org/html/rfc6455#section-5.2
				if(frameInfo.PayloadHandler != null) frameInfo.PayloadHandler.Handle(frameInfo);

			} else if((int)frameInfo.OpCode < 8) {
				//TODO: Close connection

			} else if(frameInfo.OpCode == WSFrameInfoOpCode.Close) {
				statusCode = 1001;
				statusMessage = "Connection closed by client";


			} else if(frameInfo.OpCode == WSFrameInfoOpCode.Ping) {
			} else if(frameInfo.OpCode == WSFrameInfoOpCode.Pong) {
			} else {
				//TODO: Close connection
			}



			frameInfo.ProcessedPayloadBytes += length;
		}

		internal void Send(WSClientInfo client, ISegmentGenerator generator) {
			WSFrameInfo frameInfo;
			if(!frameInfoPool.TryDequeue(out frameInfo)) frameInfo = new WSFrameInfo();
			frameInfo.OpCode = WSFrameInfoOpCode.None;
			frameInfo.Action = WSFrameInfoAction.Send;
			frameInfo.Client = client;
			frameInfo.SegmentGenerator = generator;

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

			StartSendSub(frameInfo);
			sendSAEA.SetBuffer(frameInfo.BufferOffset, frameInfo.BufferPosition);

			if(!frameInfo.Client.Socket.SendAsync(sendSAEA)) {
				ProcessSend(sendSAEA);
			}
		}
		private void StartSendSub(WSFrameInfo frameInfo) {
			if(frameInfo.MessageLength == 0) {
				frameInfo.MessageLength = frameInfo.SegmentGenerator.Length(frameInfo.Client);
				if(frameInfo.MessageLength < 0) throw new InvalidOperationException("Message length must be positive");
			}

			var maxFramePayloadLength = MaxFramePayloadLength == 0 ? Int32.MaxValue : MaxFramePayloadLength;
			if(frameInfo.MessageLength != 0) {
				maxFramePayloadLength = Math.Min(
					maxFramePayloadLength,
					(int)(frameInfo.MessageLength - frameInfo.MessageBytesTransferred)
				);
			} else {
				maxFramePayloadLength = Math.Min(
					maxFramePayloadLength,
					frameInfo.BufferLength
				);
			}

			if(frameInfo.TotalLength != 0 && frameInfo.TotalLength == frameInfo.BytesTransferred) {
				var client = frameInfo.Client;
				var messageLength = frameInfo.MessageLength;
				var messageBytesTransferred = frameInfo.MessageBytesTransferred;
				var segmentGenerator = frameInfo.SegmentGenerator;
				var buffer = frameInfo.Buffer;
				var bufferOffset = frameInfo.BufferOffset;
				var bufferLength = frameInfo.BufferLength;

				frameInfo.Reset();

				frameInfo.Action = WSFrameInfoAction.Send;
				frameInfo.OpCode = WSFrameInfoOpCode.Continuation;
				frameInfo.Client = client;
				frameInfo.MessageLength = messageLength;
				frameInfo.MessageBytesTransferred = messageBytesTransferred;
				frameInfo.SegmentGenerator = segmentGenerator;
				frameInfo.Buffer = buffer;
				frameInfo.BufferOffset = bufferOffset;
				frameInfo.BufferLength = bufferLength;
			}

			var sizeClass = 0;
			if(frameInfo.BytesTransferred == 0) { //True iff starting new frame
				if(frameInfo.OpCode == WSFrameInfoOpCode.None) {
					frameInfo.OpCode = frameInfo.SegmentGenerator.Type(frameInfo.Client);
					if(frameInfo.OpCode != WSFrameInfoOpCode.Binary && frameInfo.OpCode != WSFrameInfoOpCode.Text) {
						throw new InvalidOperationException("Only Binary or Text operations allowed");
					}
				}

				frameInfo.Buffer[frameInfo.BufferOffset] = (byte)frameInfo.OpCode;
				frameInfo.Buffer[frameInfo.BufferOffset + 1] = 0;
				if(maxFramePayloadLength < 126) {
					frameInfo.BufferPosition += 2;
					sizeClass = 1;
				} else if(maxFramePayloadLength < Int16.MaxValue) {
					frameInfo.BufferPosition += 4;
					sizeClass = 2;
				} else {
					frameInfo.BufferPosition += 10;
					sizeClass = 3;
				}

				frameInfo.HeaderLength = frameInfo.BufferPosition;
			}

			var maxSegmentFramePayloadLength = maxFramePayloadLength;
			maxSegmentFramePayloadLength = Math.Min(
				maxFramePayloadLength,
				frameInfo.BufferLength - frameInfo.BufferPosition
			);

			bool completed;
			var payloadWritten = frameInfo.SegmentGenerator.Read(
				frameInfo.Client, frameInfo.Buffer,
				frameInfo.BufferOffset + frameInfo.BufferPosition,
				maxSegmentFramePayloadLength, out completed
			);
			Console.WriteLine(
				"maxSgFrmPLength={0}, maxFrmPLength={1}, Offset={2}, Pos={3}, Completed={4}, Written={5}, Length={6}",
				maxSegmentFramePayloadLength, maxFramePayloadLength, frameInfo.BufferOffset, frameInfo.BufferPosition, completed, payloadWritten, frameInfo.BufferLength
			);

			frameInfo.PayloadLength = payloadWritten;
			frameInfo.BufferPosition += payloadWritten;
			var messagePosition = frameInfo.MessageBytesTransferred + payloadWritten;

			var isMessageLengthConsistent = !completed || frameInfo.MessageLength == 0 || frameInfo.MessageLength == messagePosition;
			isMessageLengthConsistent &= completed || frameInfo.MessageLength != 0 && frameInfo.MessageLength > messagePosition;
			if(!isMessageLengthConsistent) throw new InvalidOperationException("Message length inconsistent");

			if(completed) frameInfo.MessageLength = messagePosition;


			if(frameInfo.BytesTransferred == 0) {
				if(completed) maxFramePayloadLength = payloadWritten;

				if(frameInfo.MessageBytesTransferred + maxFramePayloadLength == frameInfo.MessageLength) {
					frameInfo.IsFinal = true;
					frameInfo.Buffer[frameInfo.BufferOffset] |= 128;
				}

				if(sizeClass == 1) {
					frameInfo.Buffer[frameInfo.BufferOffset + 1] |= (byte)maxFramePayloadLength;

				} else if(sizeClass == 2) {
					frameInfo.Buffer[frameInfo.BufferOffset + 1] |= 126;
					frameInfo.Buffer[frameInfo.BufferOffset + 2] = (byte)(maxFramePayloadLength >> 08);
					frameInfo.Buffer[frameInfo.BufferOffset + 3] = (byte)(maxFramePayloadLength >> 00);

				} else if(sizeClass == 3) {
					frameInfo.Buffer[frameInfo.BufferOffset + 1] |= 127;

					frameInfo.Buffer[frameInfo.BufferOffset + 2] = (byte)(maxFramePayloadLength >> 56);
					frameInfo.Buffer[frameInfo.BufferOffset + 3] = (byte)(maxFramePayloadLength >> 48);
					frameInfo.Buffer[frameInfo.BufferOffset + 4] = (byte)(maxFramePayloadLength >> 40);
					frameInfo.Buffer[frameInfo.BufferOffset + 5] = (byte)(maxFramePayloadLength >> 32);
					frameInfo.Buffer[frameInfo.BufferOffset + 6] = (byte)(maxFramePayloadLength >> 24);
					frameInfo.Buffer[frameInfo.BufferOffset + 7] = (byte)(maxFramePayloadLength >> 16);
					frameInfo.Buffer[frameInfo.BufferOffset + 8] = (byte)(maxFramePayloadLength >> 08);
					frameInfo.Buffer[frameInfo.BufferOffset + 9] = (byte)(maxFramePayloadLength >> 00);

				} else {
					throw new InvalidOperationException("Invalid size class");
				}
			}
		}
		private void ProcessSend(SocketAsyncEventArgs sendSAEA) {
			var frameInfo = (WSFrameInfo)sendSAEA.UserToken;
			sendSAEA.SetBuffer(frameInfo.BufferOffset, frameInfo.BufferLength);

			//TODO?: Non Concurrent Send Receive
			if(sendSAEA.SocketError == SocketError.Success) {
				frameInfo.BytesTransferred += sendSAEA.BytesTransferred;
				frameInfo.MessageBytesTransferred += sendSAEA.BytesTransferred;

				var isMessageSent = frameInfo.MessageBytesTransferred >= frameInfo.MessageLength;

				if(!isMessageSent && frameInfoPool.Count > 1) { //TODO?: A more threadsafe way needed?
					ts.TraceInformation("Starting send operation");
					StartSend(sendSAEA);

				} else {
					ts.TraceInformation("Enqueuing send operation and cleaning up old operation");
					if(isMessageSent) frameInfo.Action = WSFrameInfoAction.None;
					FinishedOperationAndContinue(sendSAEA);
				}

			} else {
				CloseClientSocketAndContinue(sendSAEA);
			}
		}

		private void CloseClientSocketAndContinue(SocketAsyncEventArgs saea) {
			var frameInfo = (WSFrameInfo)saea.UserToken;

			try {
				frameInfo.Client.Socket.Shutdown(SocketShutdown.Both);
			} catch(Exception) { }

			frameInfo.Client.Socket.Close();
			this.connectionLimiter.Release();

			frameInfo.Action = WSFrameInfoAction.None;
			FinishedOperationAndContinue(saea);
		}
		private void FinishedOperationAndContinue(SocketAsyncEventArgs saea) {
			WSFrameInfo frameInfo = (WSFrameInfo)saea.UserToken;
			if(frameInfo.Action != WSFrameInfoAction.None) {
				pendingOperations.Enqueue(frameInfo);
			} else {
				frameInfo.Reset();
				frameInfoPool.Enqueue((WSFrameInfo)saea.UserToken);
			}
			saea.UserToken = null;
			transferSAEAPool.Enqueue(saea);

			while(true) {
				saea = null;
				if(transferSAEAPool.TryDequeue(out saea) && pendingOperations.TryDequeue(out frameInfo)) {
					frameInfo.GetSAEABuffer(saea);
					saea.UserToken = frameInfo;

					switch(frameInfo.Action) {
						case WSFrameInfoAction.Handshake: InitHandshake(saea); break;
						case WSFrameInfoAction.Receive: StartReceive(saea); break;
						case WSFrameInfoAction.Send: StartSend(saea); break;
						default: throw new InvalidOperationException("Unknown WSFrameInfoAction");
					}

				} else {
					if(saea != null) transferSAEAPool.Enqueue(saea);
					break;
				}
			}

		}

		private const string ServerHandshakeFormat =
			"HTTP/1.1 101 Switching Protocols\r\n" +
			"Upgrade: WebSocket\r\n" +
			"Connection: Upgrade\r\n" +
			"Sec-WebSocket-Accept: {0}\r\n\r\n";
	}
}
