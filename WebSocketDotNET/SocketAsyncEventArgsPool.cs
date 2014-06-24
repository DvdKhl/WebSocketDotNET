using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace WebSocket {



	class SocketAsyncEventArgsPool {
		private ConcurrentBag<SocketAsyncEventArgs> pool;
		private byte[] dataBlock;

		private Action<SocketAsyncEventArgs> init;
		private Action<SocketAsyncEventArgs> reset;

		public SocketAsyncEventArgsPool(int itemCount, int itemSize, Action<SocketAsyncEventArgs> init, Action<SocketAsyncEventArgs> reset) {
			if(itemCount < 0) throw new ArgumentException("itemCount must be greater than or equal to zero", "itemCount");
			if(itemSize < 0) throw new ArgumentException("itemSize must be greater than or equal to zero", "itemSize");
			if(init == null) throw new ArgumentNullException("init");

			ItemBufferSize = itemSize;
			totalCount = itemCount;

			this.init = init;

			dataBlock = new byte[TotalCount * ItemBufferSize];

			pool = new ConcurrentBag<SocketAsyncEventArgs>();
			for(int i = 0; i < itemCount; i++) pool.Add(CreateItem(i * ItemBufferSize));

		}

		public SocketAsyncEventArgs Take() {
			SocketAsyncEventArgs saea;
			var success = pool.TryTake(out saea);

			if(success) {
				if(reset != null) reset(saea);
				return saea;
			}

			if(ItemBufferSize == 0) {
				Interlocked.Increment(ref totalCount);
				return CreateItem(0);
			} else {
				throw new InvalidOperationException("Pool is empty and ItemBufferSize is non zero");
			}
		}
		public void Add(SocketAsyncEventArgs saea) {
			pool.Add(saea);
		}

		private SocketAsyncEventArgs CreateItem(int offset) {
			var saea = new SocketAsyncEventArgs();
			if(ItemBufferSize != 0) saea.SetBuffer(dataBlock, offset, ItemBufferSize);
			if(init != null) init(saea);

			return saea;
		}

		public int TotalBufferSize { get { return dataBlock.Length; } }
		public int ItemBufferSize { get; private set; }

		public int TotalCount { get { return totalCount; } } private int totalCount;
		public int FreeCount { get { return pool.Count; } }
		public int InUseCount { get { return TotalCount - pool.Count; } }

	}
}
