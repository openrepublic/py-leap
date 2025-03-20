import os
import platform


_SHIP_BACKEND = os.environ.get('SHIP_BACKEND', 'default')


match platform.system():
    case 'Linux' if _SHIP_BACKEND != 'generic':
        from leap.ship._linux import (
            open_state_history as open_state_history
        )

    case _:
        from leap.ship._generic import (
            open_state_history as open_state_history
        )
