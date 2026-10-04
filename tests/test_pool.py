import time
import unittest

from collections import deque
from types import SimpleNamespace
from unittest import mock

from proxy.config import proxy_config
from proxy.pool import _WsPool


class _StopRotation(Exception):
    pass


def _open_ws():
    transport = SimpleNamespace(is_closing=lambda: False)
    writer = SimpleNamespace(transport=transport)
    return SimpleNamespace(_closed=False, writer=writer)


class WsPoolRotationTest(unittest.IsolatedAsyncioTestCase):
    async def test_warmup_preserves_media_ws_with_or_without_h2(self):
        for cf, secure, opted_out, media in [(True, True, False, True),
                                            (False, True, False, True),
                                            (True, False, False, True),
                                            (True, True, True, True)]:
            with self.subTest(cf=cf, secure=secure, opted_out=opted_out), \
                    mock.patch.object(proxy_config, 'dc_redirects', {2: '149.154.167.220'}), \
                    mock.patch.object(proxy_config, 'fallback_cfproxy', cf), \
                    mock.patch.object(proxy_config, 'disable_secure', not secure), \
                    mock.patch.object(proxy_config, 'cfproxy_h2_media', not opted_out), \
                    mock.patch.object(proxy_config, 'force_test_dc', False):
                pool = _WsPool()
                with mock.patch.object(pool, '_schedule_refill') as refill:
                    await pool.warmup()
                keys = [call.args[0] for call in refill.call_args_list]
                self.assertIn((2, False), keys)
                self.assertEqual((2, True) in keys, media)

    async def test_refills_partially_populated_bucket(self):
        pool = _WsPool()
        key = (2, False)
        pool._idle[key] = deque([
            (_open_ws(), time.monotonic()),
            (_open_ws(), time.monotonic()),
        ])
        sleep_calls = 0

        async def stop_after_one_iteration(_delay):
            nonlocal sleep_calls
            sleep_calls += 1
            if sleep_calls > 1:
                raise _StopRotation

        with mock.patch.object(proxy_config, 'pool_size', 4):
            with mock.patch(
                    'proxy.pool.asyncio.sleep',
                    side_effect=stop_after_one_iteration):
                with mock.patch.object(
                        pool, '_schedule_refill') as schedule_refill:
                    with self.assertRaises(_StopRotation):
                        await pool._rotate(
                            key, '149.154.167.220', ['example.com'])

        schedule_refill.assert_called_once_with(
            key, '149.154.167.220', ['example.com'])


if __name__ == '__main__':
    unittest.main()
