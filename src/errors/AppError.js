export class AppError extends Error {
  constructor(message, statusCode, code) {
    super(message);
    this.statusCode = statusCode;
    this.code = code;
  }
}

export class NotFoundError extends AppError {
  constructor(message = 'Not found.', code = 'NOT_FOUND') {
    super(message, 404, code);
  }
}

export class ValidationError extends AppError {
  constructor(message = 'Validation failed.', code = 'VALIDATION_ERROR') {
    super(message, 422, code);
  }
}

export class ConflictError extends AppError {
  constructor(message = 'Conflict.', code = 'CONFLICT') {
    super(message, 409, code);
  }
}

export class UnauthorizedError extends AppError {
  constructor(message = 'Unauthorized.', code = 'UNAUTHORIZED') {
    super(message, 401, code);
  }
}

export class ForbiddenError extends AppError {
  constructor(message = 'Forbidden.', code = 'FORBIDDEN') {
    super(message, 403, code);
  }
}

export class TooManyRequestsError extends AppError {
  constructor(message = 'Too many requests.', code = 'RATE_LIMITED', retryAfter = null) {
    super(message, 429, code);
    this.retryAfter = retryAfter;
  }
}