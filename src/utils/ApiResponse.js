export class ApiResponse {
  static success(data, statusCode = 200) {
    return { success: true, statusCode, data, error: null };
  }

  static created(data) {
    return { success: true, statusCode: 201, data, error: null };
  }

  static accepted(data){
    return { success: true, statusCode: 202, data, error: null};
  }

  static noContent(data){
    return {success: true, statusCode: 204, data, error: null};
  }

  static error(message, code, statusCode) {
    return { success: false, statusCode, data: null, error: { message, code } };
  }
}