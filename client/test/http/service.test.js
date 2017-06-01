import {expect} from 'chai';
import {setupPromiseMocking} from 'testHelpers';
import sinon from 'sinon';
import {get, post, put, request, formatQuery, __set__, __ResetDependency__} from 'http/service';

describe('http service', () => {
  setupPromiseMocking();

  describe('request', () => {
    const url = 'https://hanka.grzeska.nie.lubi.com';
    const method = 'GET';
    const status = 200;
    const token = 'token-23';

    let $fetch;
    let getLogin;
    let router;
    let lastRoute;
    let getLastRoute;
    let clearLoginFromSession;

    beforeEach(() => {
      $fetch = sinon
        .stub()
        .returns(
          Promise.resolve({status})
        );
      __set__('$fetch', $fetch);

      getLogin = sinon
        .stub()
        .returns({token});
      __set__('getLogin', getLogin);

      router = {
        goTo: sinon.spy()
      };
      __set__('router', router);

      lastRoute = {
        name: 'dd',
        params: {b: 1}
      };

      clearLoginFromSession = sinon.spy();
      __set__('clearLoginFromSession', clearLoginFromSession);

      getLastRoute = sinon.stub().returns(lastRoute);
      __set__('getLastRoute', getLastRoute);
    });

    afterEach(() => {
      __ResetDependency__('$fetch');
      __ResetDependency__('getLogin');
      __ResetDependency__('router');
      __ResetDependency__('getLastRoute');
      __ResetDependency__('clearLoginFromSession');
    });

    it('should open http request with given method and url', () => {
      request({
        url,
        method
      });

      const {method: actualMethod} = $fetch.firstCall.args[1];

      expect($fetch.calledWith(url)).to.eql(true);
      expect(actualMethod).to.eql(method);
    });

    it('should set headers', () => {
      const headers = {
        g: 1
      };

      request({
        url,
        method,
        headers
      });

      const {headers: {g}} = $fetch.firstCall.args[1];

      expect(g).to.eql(headers.g);
    });

    it('should set default Content-Type to application/json', () => {
      request({
        url,
        method
      });

      const {headers: {'Content-Type': contentType}} = $fetch.firstCall.args[1];

      expect(contentType).to.eql('application/json');
    });

    it('should provide option to override Content-Type header', () => {
      const contentType = 'text';

      request({
        url,
        method,
        headers: {
          'Content-Type': contentType
        }
      });

      const {headers: {'Content-Type': actualContentType}} = $fetch.firstCall.args[1];

      expect(actualContentType).to.eql(contentType);
    });

    it('should stringify json body objects', () => {
      const body = {
        d: 1
      };

      request({
        url,
        method,
        body
      });

      const {body: actualBody} = $fetch.firstCall.args[1];

      expect(actualBody).to.eql(JSON.stringify(body));
    });

    it('should return successful response when status is 200', (done) => {
      request({
        url,
        method
      }).then(() => {
        done();
      });

      Promise.runAll();
    });

    it('should return rejected response when status is 401', (done) => {
      $fetch.returns(
        Promise.resolve({
          status: 401
        })
      );

      request({
        url,
        method
      }).catch(() => {
        done();
      });

      Promise.runAll();
    });

    it('should redirect to login page and clear login state when response status is 401', (done) => {
      $fetch.returns(
        Promise.resolve({
          status: 401
        })
      );

      request({
        url,
        method
      }).catch(() => done());

      Promise.runAll();

      expect(clearLoginFromSession.called).to.eql(true);
      expect(router.goTo.calledWith('login', {
        name: lastRoute.name,
        params: JSON.stringify(lastRoute.params)
      })).to.eql(true);
    });

    it('should not redirect to login page when response status is 401 and current route already is login', (done) => {
      $fetch.returns(
        Promise.resolve({
          status: 401
        })
      );

      lastRoute.name = 'login';

      request({
        url,
        method
      }).catch(() => done());

      Promise.runAll();

      expect(router.goTo.called).to.eql(false);
    });

    it('should add Authorization header', () => {
      request({
        url,
        method
      });

      const Authorization = $fetch.firstCall.args[1].headers['X-Optimize-Authorization'];

      expect(Authorization).to.eql(`Bearer ${token}`);
    });

    it('should not add Authorization header when token is empty', () => {
      getLogin.returns(null);

      request({
        url,
        method
      });

      const {headers} = $fetch.firstCall.args[1];

      expect(headers).to.not.include.keys('X-Optimize-Authorization');
    });
  });

  describe('formatQuery', () => {
    it('should format query object into proper query string', () => {
      const query = {
        a: 1,
        b: '5=5'
      };

      expect(formatQuery(query)).to.eql('a=1&b=5%3D5');
    });
  });

  describe('methods shortcuts functions', () => {
    const response = 'response1';
    const url = 'http://basia-barbara-buda.eu';
    let request;

    beforeEach(() => {
      request = sinon
        .stub()
        .returns(response);

      __set__('request', request);
    });

    afterEach(() => {
      __ResetDependency__('request');
    });

    describe('put', () => {
      const body = 'hot-naked-body';

      it('should call request with correct options', () => {
        put(url, body);

        expect(request.calledWith({
          url,
          body,
          method: 'POST'
        }));
      });

      it('should use custom options', () => {
        put(url, body, {
          d: 12
        });

        expect(request.calledWith({
          url,
          body,
          method: 'POST',
          d: 12
        }));
      });

      it('should return request response', () => {
        expect(put()).to.eql(response);
      });
    });

    describe('post', () => {
      const body = 'hot-naked-body';

      it('should call request with correct options', () => {
        post(url, body);

        expect(request.calledWith({
          url,
          body,
          method: 'POST'
        }));
      });

      it('should use custom options', () => {
        post(url, body, {
          d: 12
        });

        expect(request.calledWith({
          url,
          body,
          method: 'POST',
          d: 12
        }));
      });

      it('should return request response', () => {
        expect(post()).to.eql(response);
      });
    });

    describe('get', () => {
      const query = 'q';

      it('should call request with correct options', () => {
        get(url, query);

        expect(request.calledWith({
          url,
          query,
          method: 'GET'
        }));
      });

      it('should use custom options', () => {
        const options = {
          a: 1
        };

        get(url, query, options);

        expect(request.calledWith({
          url,
          query,
          method: 'GET',
          a: 1
        }));
      });

      it('should return request response', () => {
        expect(get()).to.eql(response);
      });
    });
  });
});
