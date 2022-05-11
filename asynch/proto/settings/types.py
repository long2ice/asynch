from asynch.proto.streams.buffered import BufferedWriter
from asynch.proto.utils.compat import asbool


class SettingType:
    @classmethod
    async def write(cls, writer: BufferedWriter, value):
        raise NotImplementedError


class SettingUInt64(SettingType):
    @classmethod
    async def write(cls, writer, value):
        await writer.write_varint(int(value))


class SettingBool(SettingType):
    @classmethod
    async def write(cls, writer, value):
        await writer.write_varint(asbool(value))


class SettingString(SettingType):
    @classmethod
    async def write(cls, writer, value):
        await writer.write_str(value)


class SettingChar(SettingType):
    @classmethod
    async def write(cls, writer, value):
        await writer.write_str(
            value[0],
        )


class SettingFloat(SettingType):
    @classmethod
    async def write(cls, writer, value):
        """
        Float is written in string representation.
        """
        await writer.write_str(
            str(value),
        )


class SettingMaxThreads(SettingUInt64):
    @classmethod
    async def write(cls, writer, value):
        if value == "auto":
            value = 0
        await super(SettingMaxThreads, cls).write(writer, value)
