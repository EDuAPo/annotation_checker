import unittest
import numpy as np
from src.rules_checker import RuleChecker

class TestRuleChecker(unittest.TestCase):
    def setUp(self):
        config = {
            'rules': {
                'min_lidar_points': 5,
                'max_size': [10, 10, 5],
                'min_size': [0.1, 0.1, 0.1]
            }
        }
        self.checker = RuleChecker(config)

    def test_classify_motion_state(self):
        self.assertEqual(self.checker.classify_motion_state(0.05), 'static')
        self.assertEqual(self.checker.classify_motion_state(0.3), 'low_speed')
        self.assertEqual(self.checker.classify_motion_state(1.0), 'normal_speed')

    def test_check_object_validity(self):
        # 有效的对象
        obj = {
            'size': [2, 2, 2],
            'num_lidar_pts': 10,
            'attribute_tokens': {'Class': 'car'}
        }
        issues = self.checker.check_object(obj)
        self.assertEqual(len(issues), 0)

        # 点云数量不足
        obj_invalid = obj.copy()
        obj_invalid['num_lidar_pts'] = 3
        issues = self.checker.check_object(obj_invalid)
        self.assertTrue(any('点云数量过少' in issue for issue in issues))

if __name__ == '__main__':
    unittest.main()