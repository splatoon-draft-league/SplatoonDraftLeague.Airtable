using System;

namespace SquidDraftLeague.AirTable
{
    public class SdlAirTableException : Exception
    {
        public enum AirtableErrorType
        {
            NotFound,
            UnexpectedDuplicate,
            CommunicationError,
            Generic
        }

        public AirtableErrorType ErrorType { get; }

        public SdlAirTableException(string message, AirtableErrorType errorType) 
            : base(message)
        {
            this.ErrorType = errorType;
        }
    }
}
