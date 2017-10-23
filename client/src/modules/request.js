import {destroy, getToken} from 'credentials';

export function put(url, body, options = {}) {
  return request({
    url,
    body,
    method: 'PUT',
    ...options
  });
}

export function post(url, body, options = {}) {
  return request({
    url,
    body,
    method: 'POST',
    ...options
  });
}

export function get(url, query, options = {}) {
  return request({
    url,
    query,
    method: 'GET',
    ...options
  });
}

export function request({url, method, body, query, headers}) {
  const resourceUrl = query ? `${url}?${formatQuery(query)}` : url;

  return fetch(resourceUrl, {
    method,
    body: processBody(body),
    headers: {
      'Content-Type': 'application/json',
      ...createAuthorizationHeader(),
      ...headers
    },
    mode: 'cors'
  })
  .then(response => {
    const {status} = response;

    if (status >= 200 && status < 300) {
      return response;
    } else if (status === 401) {
      destroy();
    }
    return Promise.reject(response);
  });
}

function createAuthorizationHeader() {
  const token = getToken();

  if(token) {
    return {
      'X-Optimize-Authorization' : `Bearer ${token}`
    };
  }
  return {};
}

export function formatQuery(query) {
  return Object
    .keys(query)
    .reduce((queryStr, key) => {
      const value = query[key];

      if (queryStr === '') {
        return `${key}=${encodeURIComponent(value)}`;
      }

      return `${queryStr}&${key}=${encodeURIComponent(value)}`;
    }, '');
}

function processBody(body) {
  if (typeof body === 'string') {
    return body;
  }

  return JSON.stringify(body);
}