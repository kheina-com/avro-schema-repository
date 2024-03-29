from typing import List

import ujson
from avrofastapi.schema import AvroSchema
from kh_common.base64 import b64decode, b64encode
from kh_common.caching import AerospikeCache
from kh_common.caching.key_value_store import KeyValueStore
from kh_common.crc import CRC
from kh_common.exceptions.http_error import HttpErrorHandler, NotFound
from kh_common.sql import SqlInterface


KVS: KeyValueStore = KeyValueStore('kheina', 'avro_schemas', local_TTL=60)
crc: CRC = CRC(64)


def int_to_bytes(integer: int) -> bytes :
	return integer.to_bytes(8, 'little')


def int_from_bytes(bytestring: bytes) -> int :
	return int.from_bytes(bytestring, 'little')


class SchemaRepository(SqlInterface) :

	@HttpErrorHandler('retrieving schema')
	@AerospikeCache('kheina', 'avro_schemas', '{fingerprint}', _kvs=KVS)
	async def getSchema(self, fingerprint: str) -> AvroSchema :
		fp: int = int_from_bytes(b64decode(fingerprint))

		data: List[bytes] = await self.query_async("""
			SELECT schema
			FROM kheina.public.avro_schemas
			WHERE fingerprint = %s;
			""",
			# because crc returns unsigned, we "convert" to signed
			(fp - 9223372036854775808,),
			fetch_one=True,
		)

		if not data :
			raise NotFound('no data was found for the provided schema fingerprint.')

		return ujson.loads(data[0].tobytes())


	@HttpErrorHandler('saving schema')
	async def addSchema(self, schema: AvroSchema) -> str :
		data: bytes = ujson.dumps(schema).encode()
		fingerprint: int = crc(data)

		await self.query_async("""
			INSERT INTO kheina.public.avro_schemas
			(fingerprint, schema)
			VALUES
			(%s, %s)
			ON CONFLICT ON CONSTRAINT avro_schemas_pkey DO 
				UPDATE SET
					schema = %s;
			""",
			# because crc returns unsigned, we "convert" to signed
			(fingerprint - 9223372036854775808, data, data),
			commit=True,
		)

		fp: str = b64encode(int_to_bytes(fingerprint)).decode()
		KVS.put(fp, schema)

		return fp
