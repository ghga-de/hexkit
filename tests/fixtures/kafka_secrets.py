# Copyright 2021 - 2025 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
# for the German Human Genome-Phenome Archive (GHGA)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Generate secrets for authenticated and encrypted communication with Kafka."""

import datetime
import secrets
import string
from typing import Optional

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID, ObjectIdentifier

__all__ = ["KafkaSecrets"]


class KafkaSecrets:
    """Container for all secrets needed to establish a TLS connection with Kafka."""

    ca_cert: str

    broker_cert: str
    broker_key: str
    broker_pwd: str

    client_cert: str
    client_key: str
    client_pwd: str

    def __init__(
        self,
        hostname: str = "localhost",
        broker_pwd_size: int = 0,
        client_pwd_size: int = 16,
        days: int = 1,
    ) -> None:
        """Generate random secrets in PEM format.

        Unfortunately, the Kafka broker does not support the password protection
        algorithm provided by the cryptography library. Therefore, and because this
        is a feature that we do not need to test here, we do not generate a password
        for the broker key by default. However, is works with the Kafka client.
        """
        ca_cert, ca_key = generate_self_signed_cert(cn="ca.test.dev", days=days)
        self.ca_cert = cert_to_pem(ca_cert)

        cert, key = generate_signed_cert(
            cn=hostname, ca=ca_cert, ca_key=ca_key, client=False, days=days
        )

        self.broker_cert = cert_to_pem(cert)
        password = generate_password(broker_pwd_size)
        self.broker_key = key_to_pem(key, password)
        self.broker_pwd = password

        cert, key = generate_signed_cert(
            cn=hostname, ca=ca_cert, ca_key=ca_key, client=True, days=days
        )

        self.client_cert = cert_to_pem(cert)
        password = generate_password(client_pwd_size)
        self.client_key = key_to_pem(key, password)
        self.client_pwd = password


def cert_to_pem(cert: x509.Certificate) -> str:
    """Serialize the given certificate in PEM format."""
    return cert.public_bytes(serialization.Encoding.PEM).decode("ascii")


def key_to_pem(key: rsa.RSAPrivateKey, password: Optional[str]) -> str:
    """Serialize the given key in PEM format."""
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=get_encryption_algorithm(password),
    ).decode("ascii")


def generate_password(size: int = 16) -> str:
    """Generate a random password."""
    chars = string.ascii_letters + string.digits
    choice = secrets.choice
    return "".join(choice(chars) for _i in range(size))


def get_encryption_algorithm(
    password: Optional[str],
) -> serialization.KeySerializationEncryption:
    """Get an encryption algorithm for the given password."""
    return (
        serialization.BestAvailableEncryption(password.encode("utf-8"))
        if password
        else serialization.NoEncryption()
    )


def generate_key() -> rsa.RSAPrivateKey:
    """Generate a private key using RSA."""
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def generate_cert(
    subject: x509.Name,
    issuer: x509.Name,
    public_key: rsa.RSAPublicKey,
    signing_key: rsa.RSAPrivateKey,
    is_ca: bool,
    days: int,
    extended_key_usage: list[ObjectIdentifier],
    issuer_key: rsa.RSAPublicKey,
) -> x509.Certificate:
    """Generate a certificate with the given parameters."""
    return (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(public_key)
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.now(datetime.timezone.utc))
        .not_valid_after(
            datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=days)
        )
        .add_extension(
            x509.BasicConstraints(ca=is_ca, path_length=None),
            critical=True,
        )
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                key_encipherment=True,
                content_commitment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=is_ca,
                crl_sign=is_ca,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.ExtendedKeyUsage(extended_key_usage),
            critical=False,
        )
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(public_key),
            critical=False,
        )
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(issuer_key),
            critical=False,
        )
        .sign(signing_key, hashes.SHA256())
    )


def generate_signed_cert(
    cn: str,
    ca: x509.Certificate,
    ca_key: rsa.RSAPrivateKey,
    client: bool = False,
    days: int = 1,
) -> tuple[x509.Certificate, rsa.RSAPrivateKey]:
    """Generate a signed certificate with its private key."""
    key = generate_key()
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])
    cert = generate_cert(
        subject=subject,
        issuer=ca.subject,
        public_key=key.public_key(),
        signing_key=ca_key,
        is_ca=False,
        days=days,
        extended_key_usage=[
            ExtendedKeyUsageOID.CLIENT_AUTH
            if client
            else ExtendedKeyUsageOID.SERVER_AUTH
        ],
        issuer_key=ca_key.public_key(),  # Use the CA's public key
    )
    return cert, key


def generate_self_signed_cert(
    cn: str, days: int = 1
) -> tuple[x509.Certificate, rsa.RSAPrivateKey]:
    """Generate a self-signed certificate with its private key."""
    key = generate_key()
    subject = issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])
    cert = generate_cert(
        subject=subject,
        issuer=issuer,
        public_key=key.public_key(),
        signing_key=key,
        is_ca=True,
        days=days,
        extended_key_usage=[
            ExtendedKeyUsageOID.SERVER_AUTH,
        ],
        issuer_key=key.public_key(),  # Use the same key for self-signed cert
    )
    return cert, key
