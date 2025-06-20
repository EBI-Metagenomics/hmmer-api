import struct
import io
from django.test import SimpleTestCase

# from result.models import HmmdSearchStatus, HmmdSearchStats, P7Hit, HmmpgmdStatus, Result


# class SearchStatusTestCase(SimpleTestCase):
#     def setUp(self):
#         self.stream = io.BytesIO(struct.pack("!IQ", HmmpgmdStatus.OK.value, 123))

#     def test_should_validate(self):
#         validated = HmmdSearchStatus.from_binary(self.stream)

#         self.assertEqual(validated.status, HmmpgmdStatus.OK)
#         # self.assertEqual(validated.type, HmmpgmdResultType.SEQUENCE)
#         self.assertEqual(validated.message_size, 123)


# class SearchStatsTestCase(SimpleTestCase):
#     def test_should_validate(self):
#         with open("/Users/aleksandar/Documents/djhmmer_temp/data/results/res.dat", mode="rb") as res_file:
#             stats = HmmdSearchStats.from_binary(res_file)

#         self.assertEqual(stats.nhits, 551)
#         self.assertEqual(stats.nincluded, 400)
#         self.assertEqual(len(stats.hit_offsets), 551)


# class P7HitTestCase(SimpleTestCase):
#     def test_should_validate(self):
#         with open("/Users/aleksandar/Documents/djhmmer_temp/data/results/res.dat", mode="rb") as res_file:
#             HmmdSearchStats.from_binary(res_file)
#             hit = P7Hit.from_binary(res_file)

#         self.assertEqual(hit.ndom, 1)
#         self.assertEqual(hit.name, "200721849")


# class ResultTestCase(SimpleTestCase):
#     def test_should_validate(self):
#         result = Result(binary_file="/Users/aleksandar/Documents/djhmmer_temp/data/results/res.dat")
