"""Inert physical-upgrade guards; no seed, SDK, Docker or schema CLI executes."""
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest

import physical_upgrade as physical


class PhysicalUpgradeTest(unittest.TestCase):
    def setUp(self):
        self.profile = physical.profile()
        self.started = datetime(2026,1,1,tzinfo=timezone.utc).timestamp()
        self.before = {'schema':[['table','business','business','CREATE TABLE business(id TEXT)']],
                       'tables':{'business':{'columns':['id'],'rows':[[['text','original']]]},
                                 'dw_server_schema':{'columns':['engine','version'],
                                    'rows':[[['text','rust-development'],['integer',12]]]},
                                 '_sqlx_migrations':{'columns':['version'],'rows':[[['integer',12]]]}}}
        self.after = deepcopy(self.before)
        self.after['tables']['dw_server_schema']['rows'][0][1] = ['integer',13]
        self.after['tables']['dw_command_intents'] = {'columns':['command_id','namespace','intent_digest'],'rows':[]}
        self.after['tables']['_sqlx_migrations'] = {
            'columns':['version','description','installed_on','success','checksum','execution_time'],
            'rows':[[['integer',13],['text',self.profile['journal_description']],
                     ['text','2026-01-01 00:00:00'],['integer',1],
                     ['blob',self.profile['journal_checksum_sha384']],['integer',0]]]}
        self.after['schema'] += [[('table' if name=='dw_command_intents' else 'index'),
                                 name,'dw_command_intents',None] for name in self.profile['new_schema_objects']]
        self.observed = {'backend':'sqlite','from_schema_version':12,'schema_version':13,'status':'upgraded'}

    def validate(self, after):
        physical.validate_upgrade(self.before,after,self.observed,self.started,self.started+1,self.profile)

    def test_only_declared_version_journal_and_empty_intent_catalog_accept(self):
        self.validate(self.after)

    def test_original_business_row_or_catalog_change_refuses(self):
        changed=deepcopy(self.after);changed['tables']['business']['rows'][0][0]=['text','changed']
        with self.assertRaises(AssertionError):self.validate(changed)
        changed=deepcopy(self.after);changed['schema'].pop(0)
        with self.assertRaises(AssertionError):self.validate(changed)
        changed=deepcopy(self.after);changed['schema'].append(['index','extra','dw_command_intents',None])
        with self.assertRaises(AssertionError):self.validate(changed)

    def test_every_journal_field_and_invocation_time_are_bound(self):
        for index,value in enumerate((['integer',12],['text','other'],['text','2025-01-01 00:00:00'],
                                     ['integer',0],['blob','00'],['integer',1])):
            changed=deepcopy(self.after);changed['tables']['_sqlx_migrations']['rows'][0][index]=value
            with self.subTest(index=index), self.assertRaises(AssertionError):self.validate(changed)
        changed=deepcopy(self.after);changed['tables']['dw_command_intents']['rows']=[[]]
        with self.assertRaises(AssertionError):self.validate(changed)

    def test_unowned_shape_changed_hash_and_existing_destination_refuse(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);source=root/'input';source.write_bytes(b'owned')
            source.chmod(0o600)
            digest=physical.hashlib.sha256(source.read_bytes()).hexdigest()
            raw,_=physical.read_owned(source,digest,100)
            with self.assertRaises(ValueError):physical.read_owned(source,'0'*64,100)
            with self.assertRaises(ValueError):physical.read_owned(source,digest,2)
            link=root/'link';link.symlink_to(source)
            with self.assertRaises(ValueError):physical.read_owned(link,digest,100)
            link.unlink();link.hardlink_to(source)
            with self.assertRaises(ValueError):physical.read_owned(source,digest,100)
            link.unlink();destination=root/'copy'
            physical.copy_backup(raw,destination)
            with self.assertRaises(FileExistsError):physical.copy_backup(b'other',destination)
            self.assertEqual(raw,source.read_bytes())
            self.assertEqual(raw,destination.read_bytes())

    def test_null_key_signal_is_compatibility_only_and_cannot_be_relabelled(self):
        packet={'cases':[{'kind':'signal','workflow_id':'workflow','receipt':{'command_id':'signal','run_id':'run'}}]}
        snapshot={'tables':{'workflow_commands':{
            'columns':['id','command_type','workflow_instance_id','workflow_run_id','request_id'],
            'rows':[[['text','signal'],['text','signal'],['text','workflow'],['text','run'],['null']]]}}}
        physical.validate_original_commands(snapshot,packet)
        snapshot['tables']['workflow_commands']['rows'][0][-1]=['text','manufactured']
        with self.assertRaises(AssertionError):physical.validate_original_commands(snapshot,packet)


if __name__=='__main__':unittest.main()
