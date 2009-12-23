#!/usr/bin/env python
#Copyright 2009 Sebastian Hagen
# This file is part of gonium.
#
# gonium is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.
#
# gonium is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

# Dummy certificates for blockfd ssl selftesting. Kept in a seperate file to
# not clutter up the memory of programs that don't want to run the selftests.

dummy_ca_crt = b'''-----BEGIN CERTIFICATE-----
MIIDGTCCAoKgAwIBAgIJAOmSw9MYObHeMA0GCSqGSIb3DQEBBQUAMGYxCzAJBgNV
BAYTAkFVMRMwEQYDVQQIEwpTb21lLVN0YXRlMRYwFAYDVQQKEw1tZW1lc3BhY2Uu
bmV0MQ8wDQYDVQQLEwZnb25pdW0xGTAXBgNVBAMTEGdvbml1bSB0ZXN0IGNlcnQw
IRcNMDkxMjE5MjIzMDUwWhgQMTgzODMwMjAyMjIzMDUwWjBmMQswCQYDVQQGEwJB
VTETMBEGA1UECBMKU29tZS1TdGF0ZTEWMBQGA1UEChMNbWVtZXNwYWNlLm5ldDEP
MA0GA1UECxMGZ29uaXVtMRkwFwYDVQQDExBnb25pdW0gdGVzdCBjZXJ0MIGfMA0G
CSqGSIb3DQEBAQUAA4GNADCBiQKBgQCsWxtbG7c2Q4nL4yw9wUvdj+Rmv7s6JRJ4
NhAM7HP8Ff4p1gO2piR+E5Avid5Twn2ZWXEaLRQuirrulrVxSdUXG5eq4Eu4Fj7y
e+lyhPJgldVPU9qfCG/kUeFL42/3EeiY+ZR9H0pTW6PO2Lk8uHtTnDhtCxsMSubK
Kra3r5Nz9QIDAQABo4HLMIHIMB0GA1UdDgQWBBRttFcFGmsjjJwgqq7ov6ZMislT
szCBmAYDVR0jBIGQMIGNgBRttFcFGmsjjJwgqq7ov6ZMislTs6FqpGgwZjELMAkG
A1UEBhMCQVUxEzARBgNVBAgTClNvbWUtU3RhdGUxFjAUBgNVBAoTDW1lbWVzcGFj
ZS5uZXQxDzANBgNVBAsTBmdvbml1bTEZMBcGA1UEAxMQZ29uaXVtIHRlc3QgY2Vy
dIIJAOmSw9MYObHeMAwGA1UdEwQFMAMBAf8wDQYJKoZIhvcNAQEFBQADgYEAG7Ff
LlEKSv0li3o2nFbmzUl7T4MLuvyNymywIC7Qnpu1CvPxmewNloiS1b6DBBHcYbce
SakbXymbU/MbYYKSiZ3ova4gQEWdXWP+X4rvSLEJ5I3FOzEO5jdJqoU4Sy4BUZ5/
UOZErFTaNPLOb6cIjsn917uJR0FT9ZZdyT6fhK8=
-----END CERTIFICATE-----'''

dummy_ca_key = b'''-----BEGIN RSA PRIVATE KEY-----
MIICXQIBAAKBgQCsWxtbG7c2Q4nL4yw9wUvdj+Rmv7s6JRJ4NhAM7HP8Ff4p1gO2
piR+E5Avid5Twn2ZWXEaLRQuirrulrVxSdUXG5eq4Eu4Fj7ye+lyhPJgldVPU9qf
CG/kUeFL42/3EeiY+ZR9H0pTW6PO2Lk8uHtTnDhtCxsMSubKKra3r5Nz9QIDAQAB
AoGBAIGP5yuA1SO8d2xF9C7kDFScYzR98o9N6OlmsoAUi0e3fJ0UXSxDDnGb4Spr
OC68qE/LdYMY2e/2p3jM384ukziUZ2bLo459yFEwDLkNF8Eo6H07kb0PF6shEyjG
mp6ytOTQqfVtMEdJu9tGSOv0aKQr46zuEFIUmsRKDRKbUvRtAkEA3bv9L4dYD10h
vWI7xG/Ad31TvuNdI+mJevrZDuxE/q42vT8RlS9syvlEEGLQ5nhEG/4weJeqp4f/
P8mk29n+ewJBAMb9qWyOt278zOb3iINNkwEtnal/Nl+HqHGRs5K54xyZaZyVE0nZ
HKYnCYA8mAjh+N4se5MdXCgHlmIWdZe8BE8CQQCIq5WsVQT/O01PmRvzwVnljLcw
wp2nRAw8ZB+kJheEz0boMNaamMe6+Bdu7imK1PhONMgVnI5Qgu/elmBqLpy/AkAk
GtRJyR9JOP8ojRMs179rgE5R+a3w6VlLueb+IVIu1zsNWRMV3BnRn9WeeeAQBIg2
L5YAXFxjOvUzOOX/MPMLAkBjDoo6ou6lTTGTkDTWgdF+yP8Oc8MWAhmD/XmGP3j5
gcd+wIXj6DVwTRD5kMPa8nvv3Z+nwNnGB7DO0t8QsFEb
-----END RSA PRIVATE KEY-----'''
