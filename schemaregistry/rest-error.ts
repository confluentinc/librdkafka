/**
 * Represents a REST error.
 */
export class RestError extends Error {
  status: number;
  errorCode: number;

  /**
   * Creates a new REST error.
   * @param message - The error message.
   * @param status - The HTTP status code.
   * @param errorCode - The error code.
   */
  constructor(message: string, status: number, errorCode: number) {
    super(message + "; Error code: " + errorCode);
    this.status = status;
    this.errorCode = errorCode;
  }
}
