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


class SchemaRepository(SqlInterface) :

	@HttpErrorHandler('retrieving schema')
	@AerospikeCache('kheina', 'avro_schemas', '{fingerprint}', _kvs=KVS)
	async def getSchema(self, fingerprint: str) -> AvroSchema :
		fp: int = b64decode(fingerprint)

		data: List[bytes] = await self.query_async("""
			SELECT schema
			FROM kheina.public.avro_schemas
			WHERE fingerprint = %s;
			""",
			(fp,),
			fetch_one=True,
		)

		if not data :
			raise NotFound('no data was found for the provided schema fingerprint.')

		return ujson.loads(data[0])


	@HttpErrorHandler('saving schema')
	async def addSchema(self, schema: AvroSchema) -> int :
		data: bytes = ujson.dumps(schema)
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
			(fingerprint, data, data),
			commit=True,
		)

		fp: str = b64encode(fingerprint)
		KVS.put(fp, schema)

		return fp
