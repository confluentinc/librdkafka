export class RestError extends Error {
  status: number;
  errorCode: number;

  constructor(message: string, status: number, errorCode: number) {
    super(message + "; Error code: " + errorCode);
    this.status = status;
    this.errorCode = errorCode;
  }
}