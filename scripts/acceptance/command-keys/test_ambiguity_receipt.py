"""An ambiguity refusal must not silently select a retained command receipt."""
from copy import deepcopy
import unittest

import run_open
import sqlite_legacy


class AmbiguityReceiptTest(unittest.TestCase):
    def setUp(self):
        self.expected = sqlite_legacy.manifest()['ambiguity_expected']
        self.result = {'kind':'ok','value':{'status':409,'body':{
            'request_id':'synthetic-key','reason':'request_id_ambiguous',
            'retained':True,'intent_verified':False,'retryable':False}}}

    def test_exact_unverified_nonretryable_refusal_accepts(self):
        self.assertEqual(self.result['value']['body'], run_open.ambiguity_receipt(
            self.result, 'synthetic-key', self.expected))
        # The frozen ambiguity contract does not require an echoed caller key.
        # Error::request_id() likewise reads only a key actually echoed by HTTP.
        absent = deepcopy(self.result)
        del absent['value']['body']['request_id']
        self.assertEqual(absent['value']['body'], run_open.ambiguity_receipt(
            absent, 'synthetic-key', self.expected))

    def test_selected_original_or_alternative_receipt_refuses(self):
        for field in ('command_id','run_id','update_id','accepted_at','retained_command_id','receipt'):
            result = deepcopy(self.result)
            result['value']['body'][field] = 'selected-identity'
            with self.subTest(field=field), self.assertRaises(AssertionError):
                run_open.ambiguity_receipt(result, 'synthetic-key', self.expected)

    def test_success_conflict_wrong_key_or_verified_intent_cannot_qualify_as_ambiguous(self):
        for field,value in (('reason','request_id_conflict'), ('request_id','other-key'),
                            ('retained',False), ('retained',1), ('intent_verified',True),
                            ('intent_verified',0), ('retryable',True)):
            result = deepcopy(self.result)
            result['value']['body'][field] = value
            with self.subTest(field=field,value=value), self.assertRaises(AssertionError):
                run_open.ambiguity_receipt(result, 'synthetic-key', self.expected)
        result = deepcopy(self.result)
        result['value']['status'] = 202
        with self.assertRaises(AssertionError):
            run_open.ambiguity_receipt(result, 'synthetic-key', self.expected)


if __name__ == '__main__':
    unittest.main()
