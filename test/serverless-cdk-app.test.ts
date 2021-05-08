import { expect as expectCDK, matchTemplate, MatchStyle } from '@aws-cdk/assert';
import * as cdk from '@aws-cdk/core';
import * as ServerlessCdkApp from '../lib/serverless-cdk-app-stack';

test('Empty Stack', () => {
    const app = new cdk.App();
    // WHEN
    const stack = new ServerlessCdkApp.ServerlessCdkAppStack(app, 'MyTestStack');
    // THEN
    expectCDK(stack).to(matchTemplate({
      "Resources": {}
    }, MatchStyle.EXACT))
});
