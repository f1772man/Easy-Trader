# Changelog

## [2026-03-28]
### Fixed
- `firebase.py`: Handle RemoteDisconnected in RTDB updates with silent retry
- `domestic_stock_functions.py`: Fix kis_auth import path for Docker environment

### Added
- `engine.py`: Skip warmup and trading on market holidays via chk_holiday API
