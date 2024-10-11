import { ModuleOptions, ClientCredentials, ClientCredentialTokenConfig, AccessToken } from 'simple-oauth2';

const TOKEN_EXPIRATION_THRESHOLD_SECONDS = 30 * 60; // 30 minutes

export class OAuthClient {
  private client: ClientCredentials;
  private token: AccessToken | undefined;
  private tokenParams: ClientCredentialTokenConfig;

  constructor(clientId: string, clientSecret: string, tokenHost: string, tokenPath: string, scope: string) {
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
  }

  async getAccessToken(): Promise<string> {
    if (!this.token || this.token.expired(TOKEN_EXPIRATION_THRESHOLD_SECONDS)) {
      await this.generateAccessToken();
    }

    return this.getAccessTokenString();
  }

  async generateAccessToken(): Promise<void> {
    try {
      const token = await this.client.getToken(this.tokenParams);
      this.token = token;
    } catch (error) {
      if (error instanceof Error) {
        throw new Error(`Failed to get token from server: ${error.message}`);
      }
      throw new Error(`Failed to get token from server: ${error}`);
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
