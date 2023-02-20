import { Injectable, CanActivate, ExecutionContext, UnauthorizedException } from '@nestjs/common';
import * as jwt from 'jsonwebtoken';

@Injectable()
export class JwtGuard implements CanActivate {
  async canActivate(context: ExecutionContext): Promise<boolean> {
    const request = context.switchToHttp().getRequest();
    if (!request.headers.authorization) {
      return false;
    }
    request.user = await this.validateToken(request.headers.authorization);
    return true;
  }

  async validateToken(auth: string) {
    if (auth.split(' ')[0] !== 'Bearer') {
      throw new Error('Invalid token');
    }
    const token = auth.split(' ')[1];
    try {
      const decoded = jwt.verify(token, process.env.SECRET);
      return decoded;
    } catch (err) {
      const message = 'Token error: ' + (err.message || err.name);
      throw new UnauthorizedException('You are not authorized');
    }
  }
}
