import logging
from typing import Dict

from asynch.proto.settings.available import settings as available_settings
from asynch.proto.streams.buffered import BufferedWriter

logger = logging.getLogger(__name__)


async def write_settings(
    writer: BufferedWriter, settings: Dict, settings_as_strings, settings_is_important: bool
):

    for setting, value in (settings or {}).items():
        # If the server support settings as string we do not need to know
        # anything about them, so we can write any setting.
        if settings_as_strings:
            await writer.write_str(setting)
            await writer.write_uint8(int(settings_is_important))
            await writer.write_str(str(value))

        else:
            # If the server requires string in binary,
            # then they cannot be written without type.
            setting_writer = available_settings.get(setting)
            if not setting_writer:
                logger.warning("Unknown setting %s. Skipping", setting)
                continue
            await writer.write_str(
                setting,
            )
            await setting_writer.write(writer, value)

    await writer.write_str("")  # end of settings
