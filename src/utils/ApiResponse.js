export class ApiResponse {
  static success(data, statusCode = 200) {
    return { success: true, statusCode, data, error: null };
  }

  static created(data) {
    return { success: true, statusCode: 201, data, error: null };
  }

  static error(message, code, statusCode) {
    return { success: false, statusCode, data: null, error: { message, code } };
  }
}