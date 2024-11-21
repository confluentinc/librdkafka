import { OAuthClient } from '../oauth/oauth-client';
import { ClientCredentials, AccessToken } from 'simple-oauth2';
import { beforeEach, afterEach, describe, expect, it, jest } from '@jest/globals';
import * as retryHelper from '@confluentinc/schemaregistry/retry-helper';
import { maxRetries, retriesWaitMs, retriesMaxWaitMs } from './test-constants';
import { boomify } from '@hapi/boom';

jest.mock('simple-oauth2');

const mockError = boomify(new Error('Error Message'), { statusCode: 429 });
const mockErrorNonRetry = boomify(new Error('Error Message'), { statusCode: 401 });

describe('OAuthClient', () => {
  const clientId = 'clientId';
  const clientSecret = 'clientSecret';
  const tokenHost = 'https://example.com';
  const tokenPath = '/token';
  const scope = 'scope';

  let oauthClient: OAuthClient;
  let clientCredentials: jest.MockedClass<typeof ClientCredentials>;

  const mockToken: AccessToken = {
    token: { access_token: 'mockAccessToken' },
    expired: (number: number) => false,
    refresh: jest.fn(async (params, httpOptions) => {
      return {
        ...mockToken,
        token: {
          ...mockToken.token,
          access_token: 'newMockAccessToken',
        },
      };
    }),
    revoke: jest.fn(async (tokenType, httpOptions) => {
      console.log(`Revoke called for token type: ${tokenType}`);
    }),
    revokeAll: jest.fn(async (httpOptions) => {
      console.log(`Revoke all tokens called`);
    }),
  };

  const mockTokenExpired: AccessToken = {
    token: { access_token: 'mockAccessToken' },
    expired: (number: number) => true,
    refresh: jest.fn(async (params, httpOptions) => {
      return {
        ...mockToken,
        token: {
          ...mockToken.token,
          access_token: 'newMockAccessToken',
        },
      };
    }),
    revoke: jest.fn(async (tokenType, httpOptions) => {
      console.log(`Revoke called for token type: ${tokenType}`);
    }),
    revokeAll: jest.fn(async (httpOptions) => {
      console.log(`Revoke all tokens called`);
    }),
  };

  beforeEach(() => {
    oauthClient = new OAuthClient(
      clientId,
      clientSecret,
      tokenHost,
      tokenPath,
      scope,
      maxRetries,
      retriesWaitMs,
      retriesMaxWaitMs
    );

    clientCredentials = ClientCredentials as jest.MockedClass<typeof ClientCredentials>;
    jest.spyOn(retryHelper, 'isRetriable');
    jest.spyOn(retryHelper, 'fullJitter');
    jest.spyOn(retryHelper, 'sleep');

    jest.spyOn(oauthClient, 'generateAccessToken');
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('should retrieve an access token successfully', async () => {

    clientCredentials.prototype.getToken.mockResolvedValue(mockToken);

    const token = await oauthClient.getAccessToken();
    expect(token).toBe('mockAccessToken');
  });

  it('should retry on retriable errors and succeed', async () => {

    // Fail twice with retriable errors, then succeed
    clientCredentials.prototype.getToken
      .mockRejectedValueOnce(mockError)
      .mockRejectedValueOnce(mockError)
      .mockResolvedValue(mockToken);

    const token = await oauthClient.getAccessToken();

    expect(token).toBe('mockAccessToken');
    expect(retryHelper.fullJitter).toHaveBeenCalledTimes(maxRetries);
    expect(retryHelper.fullJitter).toHaveBeenCalledWith(retriesWaitMs, retriesMaxWaitMs, 0);
    expect(retryHelper.fullJitter).toHaveBeenCalledWith(retriesWaitMs, retriesMaxWaitMs, 1);
    
    expect(retryHelper.isRetriable).toHaveBeenCalledTimes(maxRetries);
    expect(retryHelper.sleep).toHaveBeenCalledTimes(maxRetries);
  });

  it('should fail immediately on non-retriable errors', async () => {
    clientCredentials.prototype.getToken.mockRejectedValueOnce(mockErrorNonRetry);
    await expect(oauthClient.getAccessToken()).rejects.toThrowError();

    expect(retryHelper.isRetriable).toHaveBeenCalledTimes(1);
    expect(retryHelper.fullJitter).not.toHaveBeenCalled();
    expect(retryHelper.sleep).not.toHaveBeenCalled();
  });

  it('should fail after exhausting all retries', async () => {
    clientCredentials.prototype.getToken.mockRejectedValue(mockError);

    await expect(oauthClient.getAccessToken()).rejects.toThrowError();


    expect(retryHelper.isRetriable).toHaveBeenCalledTimes(maxRetries);

    expect(retryHelper.fullJitter).toHaveBeenCalledTimes(maxRetries);
    expect(retryHelper.fullJitter).toHaveBeenCalledWith(retriesWaitMs, retriesMaxWaitMs, 0);
    expect(retryHelper.fullJitter).toHaveBeenCalledWith(retriesWaitMs, retriesMaxWaitMs, 1);
    expect(retryHelper.sleep).toHaveBeenCalledTimes(maxRetries);
  });

  it('should not refresh token when not expired', async () => {
    clientCredentials.prototype.getToken.mockResolvedValue(mockToken);

    await oauthClient.getAccessToken();
    await oauthClient.getAccessToken();

    expect(oauthClient.generateAccessToken).toHaveBeenCalledTimes(1);
  });

  it('should refresh token when expired', async () => {
    clientCredentials.prototype.getToken.mockResolvedValue(mockTokenExpired);

    await oauthClient.getAccessToken();
    await oauthClient.getAccessToken();

    expect(oauthClient.generateAccessToken).toHaveBeenCalledTimes(2);
  });
});
