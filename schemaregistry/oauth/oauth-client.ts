import { ModuleOptions, ClientCredentials, ClientCredentialTokenConfig, AccessToken } from 'simple-oauth2';
import { sleep, fullJitter, isRetriable } from '../retry-helper';
import { isBoom } from '@hapi/boom';

const TOKEN_EXPIRATION_THRESHOLD_SECONDS = 30 * 60; // 30 minutes

export class OAuthClient {
  private client: ClientCredentials;
  private token: AccessToken | undefined;
  private tokenParams: ClientCredentialTokenConfig;
  private maxRetries: number;
  private retriesWaitMs: number;
  private retriesMaxWaitMs: number;

  constructor(clientId: string, clientSecret: string, tokenHost: string, tokenPath: string, scope: string,
    maxRetries: number, retriesWaitMs: number, retriesMaxWaitMs: number
  ) {
    const clientConfig: ModuleOptions = {
      client: {
        id: clientId,
        secret: clientSecret,
      },
      auth: {
        tokenHost: tokenHost,
        tokenPath: tokenPath
      }
    }

    this.tokenParams = { scope };

    this.client = new ClientCredentials(clientConfig);

    this.maxRetries = maxRetries;
    this.retriesWaitMs = retriesWaitMs;
    this.retriesMaxWaitMs = retriesMaxWaitMs;
  }

  async getAccessToken(): Promise<string> {
    if (!this.token || this.token.expired(TOKEN_EXPIRATION_THRESHOLD_SECONDS)) {
      await this.generateAccessToken();
    }

    return this.getAccessTokenString();
  }

  async generateAccessToken(): Promise<void> {
    for (let i = 0; i < this.maxRetries + 1; i++) {
      try {
        const token = await this.client.getToken(this.tokenParams);
        this.token = token;
      } catch (error: any) {
        if (isBoom(error) && i < this.maxRetries) {
          const statusCode = error.output.statusCode;
          if (isRetriable(statusCode)) {
            const waitTime = fullJitter(this.retriesWaitMs, this.retriesMaxWaitMs, i);
            await sleep(waitTime);
            continue;
          }
        } 
        throw new Error(`Failed to get token from server: ${error}`);
      }
    }
  }

  async getAccessTokenString(): Promise<string> {
    const accessToken = this.token?.token?.['access_token'];

    if (typeof accessToken === 'string') {
      return accessToken;
    }

    throw new Error('Access token is not available');
  }
}

