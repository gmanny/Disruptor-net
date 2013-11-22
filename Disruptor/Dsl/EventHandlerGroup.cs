using System;
using System.Linq;

namespace Disruptor.Dsl
{
    ///<summary>
    ///  A group of <see cref="IEventProcessor"/>s used as part of the <see cref="Disruptor"/>
    ///</summary>
    ///<typeparam name="T">the type of event used by <see cref="IEventProcessor"/>s.</typeparam>
    public class EventHandlerGroup<T> where T : class
    {
        private readonly Disruptor<T> _disruptor;
        private readonly ConsumerRepository<T> _consumerRepository;
        private readonly Sequence[] _sequences;

        internal EventHandlerGroup(Disruptor<T> disruptor, ConsumerRepository<T> consumerRepository, Sequence[] sequences)
        {
            _disruptor = disruptor;
            _consumerRepository = consumerRepository;
            _sequences = sequences;
        }

        /**
         * Create a new event handler group that combines the consumers in this group with <tt>otherHandlerGroup</tt>.
         *
         * @param otherHandlerGroup the event handler group to combine.
         * @return a new EventHandlerGroup combining the existing and new consumers into a single dependency group.
         */
        /// <summary>
        /// Create a new <see cref="EventHandlerGroup{T}"/> that combines the consumers in this group with <paramref name="otherHandlerGroup"/>.
        /// </summary>
        /// <param name="otherHandlerGroup">the event handler group to combine.</param>
        /// <returns>a new <see cref="EventHandlerGroup{T}"/> combining the existing and new consumers into a single dependency group.</returns>
        public EventHandlerGroup<T> And(EventHandlerGroup<T> otherHandlerGroup)
        {
            Sequence[] combinedSequences = new Sequence[_sequences.Length + otherHandlerGroup._sequences.Length];
            Array.Copy(_sequences, 0, combinedSequences, 0, _sequences.Length);
            Array.Copy(otherHandlerGroup._sequences, 0, combinedSequences, _sequences.Length, otherHandlerGroup._sequences.Length);
            return new EventHandlerGroup<T>(_disruptor, _consumerRepository, combinedSequences);
        }

        /// <summary>
        /// Create a new <see cref="EventHandlerGroup{T}"/> that combines the handlers in this group with input processors.
        /// </summary>
        /// <param name="processors">the processors to combine.</param>
        /// <returns>a new <see cref="EventHandlerGroup{T}"/> combining the existing and new processors into a single dependency group.</returns>
        public EventHandlerGroup<T> And(params IEventProcessor[] processors)
        {
            Sequence[] combinedSequences = new Sequence[_sequences.Length + processors.Length];

            for (int i = 0; i < processors.Length; i++)
            {
                _consumerRepository.Add(processors[i]);
                combinedSequences[i] = processors[i].Sequence;
            }
            Array.Copy(_sequences, 0, combinedSequences, processors.Length, _sequences.Length);

            return new EventHandlerGroup<T>(_disruptor, _consumerRepository, combinedSequences);
        }

        /// <summary>
        /// Set up batch handlers to consume events from the ring buffer. These handlers will only process events
        /// after every <see cref="IEventProcessor"/> in this group has processed the event.
        /// </summary>
        /// <param name="handlers">the batch handlers that will process events.</param>
        /// <returns>a <see cref="EventHandlerGroup{T}"/> that can be used to set up an event processor barrier over the created event processors.</returns>
        public EventHandlerGroup<T> Then(params IEventHandler<T>[] handlers)
        {
            return HandleEventsWith(handlers);
        }

        /// <summary>
        /// Set up batch handlers to consume events from the ring buffer. These handlers will only process events
        /// after every <see cref="IEventProcessor"/> in this group has processed the event.
        /// </summary>
        /// <param name="handlers">the batch handlers that will process events.</param>
        /// <returns>a <see cref="EventHandlerGroup{T}"/> that can be used to set up an event processor barrier over the created event processors.</returns>
        public EventHandlerGroup<T> ThenHandleEventsWithWorkerPool(params IWorkHandler<T>[] handlers)
        {
            return HandleEventsWithWorkerPool(handlers);
        }

        /// <summary>
        /// Set up batch handlers to handle events from the ring buffer. These handlers will only process events
        /// after every <see cref="IEventProcessor"/>s in this group has processed the event.
        /// </summary>
        /// <param name="handlers">the batch handlers that will process events.</param>
        /// <returns>a <see cref="EventHandlerGroup{T}"/> that can be used to set up a event processor barrier over the created event processors.</returns>
        public EventHandlerGroup<T> HandleEventsWith(params IEventHandler<T>[] handlers)
        {
            return _disruptor.CreateEventProcessors(_sequences, handlers);
        }

        /// <summary>
        /// Set up a worker pool to handle events from the ring buffer. The worker pool will only process events
        /// after every <see cref="IEventProcessor"/>s in this group has processed the event. Each event will be processed
        /// by one of the work handler instances.
        /// </summary>
        /// <param name="handlers">the work handlers that will process events. Each work handler instance will provide an extra thread in the worker pool.</param>
        /// <returns>a <see cref="EventHandlerGroup{T}"/> that can be used to set up a event processor barrier over the created event processors.</returns>
        public EventHandlerGroup<T> HandleEventsWithWorkerPool(params IWorkHandler<T>[] handlers)
        {
            return _disruptor.CreateWorkerPool(_sequences, handlers);
        }

        /// <summary>
        /// Create a <see cref="ISequenceBarrier"/> for the processors in this group.
        /// This allows custom event processors to have dependencies on
        /// <see cref="BatchEventProcessor{T}"/>s created by the disruptor.
        /// </summary>
        /// <returns></returns>
        public ISequenceBarrier AsSequenceBarrier()
        {
            return _disruptor.RingBuffer.NewBarrier(_sequences);
        }
    }
}
