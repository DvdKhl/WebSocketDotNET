using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace WebSocket.Streams {
	//Hacky attempt to support WSS
	public class WSSecureStream : Stream {
		private byte[] buffer;
		private int bufferOffset, bufferLength;

		private NetworkStream nwStream;
		private bool isTLSHandshaking = true;
		private Socket socket;
		private object ServerCertificatePath;

		public int ReadPosition { get; private set; }
		public int WritePosition { get; private set; }

		public WSSecureStream(Socket socket) {
			nwStream = new NetworkStream(socket, false);
		}

		public WSSecureStream(Socket socket, object serverCertificatePath) {
			// TODO: Complete member initialization
			this.socket = socket;
			this.ServerCertificatePath = serverCertificatePath;
			//frameInfo.Owner.SecureStream = new WSSecureStream(receiveSAEA.AcceptSocket);
			//frameInfo.Owner.TLSStream = new SslStream(frameInfo.Owner.SecureStream, true);
			//frameInfo.Owner.TLSStream.AuthenticateAsServer(X509Certificate2.CreateFromCertFile(@"D:\Certs\DvdKhlDevCA_localhost.cer"));
			//frameInfo.Owner.TLSStream.Flush();
			//frameInfo.Owner.SecureStream.FinishedTLSHandshaking();
		}

		public void SetBuffer(byte[] buffer, int offset, int length) {
			this.buffer = buffer;
			bufferOffset = offset;
			bufferLength = length;
			WritePosition = 0;
			ReadPosition = 0;
		}
		public void SetBuffer(int offset, int length) {
			bufferOffset = offset;
			bufferLength = length;
			WritePosition = 0;
			ReadPosition = 0;
		}



		public override int Read(byte[] buffer, int offset, int count) {
			if(isTLSHandshaking) {
				var ret = nwStream.Read(buffer, offset, count);
				Console.WriteLine("nwStream.Read({0}, {1}, {2}) == {3}", buffer.Length, offset, count, ret);
				return ret;
			} else {
				var bytesCopied = Math.Min(bufferLength - ReadPosition, count);
				Buffer.BlockCopy(this.buffer, bufferOffset + ReadPosition, buffer, offset, bytesCopied);
				ReadPosition += bytesCopied;
				Console.WriteLine("base.Read({0}, {1}, {2}) == {3}", buffer.Length, offset, count, bytesCopied);

				return bytesCopied;
			}
		}

		public override void Write(byte[] buffer, int offset, int count) {
			if(isTLSHandshaking) {
				Console.WriteLine("nwStream.Write({0}, {1}, {2})", buffer.Length, offset, count);
				nwStream.Write(buffer, offset, count);
			} else {
				Buffer.BlockCopy(buffer, offset, this.buffer, bufferOffset + WritePosition, count);
				WritePosition += count;
				Console.WriteLine("base.Write({0}, {1}, {2})", buffer.Length, offset, count);
			}
		}

		public override void Flush() { }
		public override bool CanRead { get { return true; } }
		public override bool CanSeek { get { return false; } }
		public override bool CanWrite { get { return true; } }
		public override long Length { get { throw new NotSupportedException(); } }
		public override long Seek(long offset, SeekOrigin origin) { throw new NotSupportedException(); }
		public override void SetLength(long value) { throw new NotSupportedException(); }
		public override long Position { get { throw new NotSupportedException(); } set { throw new NotSupportedException(); } }

		internal int DecryptInPlace(byte[] p1, int p2, int p3) {
			throw new NotImplementedException();
			//frameInfo.Owner.SecureStream.SetBuffer(receiveSAEA.Buffer, receiveSAEA.Offset, receiveSAEA.BytesTransferred);
			//
			//int bytesRead = 0, totalBytesRead = 0;
			//do {
			//	bytesRead = frameInfo.Owner.TLSStream.Read(receiveSAEA.Buffer, receiveSAEA.Offset + totalBytesRead, settings.TransferBufferSize);
			//	totalBytesRead += bytesRead;
			//} while(bytesRead != 0);
			//
			//
			//
			//frameInfo.BufferLength += totalBytesRead;
			//var sdgf = Encoding.UTF8.GetString(receiveSAEA.Buffer, receiveSAEA.Offset, totalBytesRead);
		}

		public bool IsConnected { get; set; }

		internal int EncryptInPlace(byte[] p1, int p2, int p3) {
			throw new NotImplementedException();
			//frameInfo.Owner.SecureStream.SetBuffer(sendSAEA.Buffer, sendSAEA.Offset, rspByteCount);
			//frameInfo.Owner.TLSStream.Write(sendSAEA.Buffer, sendSAEA.Offset, rspByteCount);
			//frameInfo.Owner.TLSStream.Flush();
			//rspByteCount = frameInfo.Owner.SecureStream.WritePosition;
		}
	}
}
