"""Tests for the file/batch state-machine transition tables."""

from __future__ import annotations

import pytest

from filemig.domain.states import (
    BatchState,
    FileState,
    IllegalTransition,
    assert_batch_transition,
    assert_file_transition,
    recovery_target,
)


def test_happy_path_file_transitions():
    chain = [
        FileState.DISCOVERED, FileState.QUEUED, FileState.DOWNLOADING,
        FileState.DOWNLOADED, FileState.UPLOADING, FileState.UPLOADED,
        FileState.VERIFYING, FileState.VERIFIED, FileState.DELETING, FileState.DONE,
    ]
    for frm, to in zip(chain, chain[1:]):
        assert_file_transition(frm, to)  # must not raise


def test_cannot_skip_to_deleting_without_verified():
    with pytest.raises(IllegalTransition):
        assert_file_transition(FileState.UPLOADED, FileState.DELETING)
    with pytest.raises(IllegalTransition):
        assert_file_transition(FileState.DOWNLOADED, FileState.DELETING)


def test_verified_only_reaches_deleting():
    assert_file_transition(FileState.VERIFIED, FileState.DELETING)
    with pytest.raises(IllegalTransition):
        assert_file_transition(FileState.VERIFIED, FileState.DONE)
    with pytest.raises(IllegalTransition):
        assert_file_transition(FileState.VERIFIED, FileState.UPLOADING)


def test_dead_is_terminal_except_manual_revival():
    assert_file_transition(FileState.DEAD, FileState.QUEUED)
    with pytest.raises(IllegalTransition):
        assert_file_transition(FileState.DEAD, FileState.DELETING)


def test_done_is_terminal():
    with pytest.raises(IllegalTransition):
        assert_file_transition(FileState.DONE, FileState.QUEUED)


def test_idempotency_shortcircuit_allowed():
    # A file already present on Drive can jump queued -> uploaded/verified.
    assert_file_transition(FileState.QUEUED, FileState.UPLOADED)
    assert_file_transition(FileState.QUEUED, FileState.VERIFIED)


def test_batch_happy_path():
    chain = [
        BatchState.PROPOSED, BatchState.COPY_APPROVED, BatchState.RUNNING,
        BatchState.AWAITING_DELETE_APPROVAL, BatchState.DELETING, BatchState.DONE,
    ]
    for frm, to in zip(chain, chain[1:]):
        assert_batch_transition(frm, to)


def test_batch_cancel_only_while_proposed():
    assert_batch_transition(BatchState.PROPOSED, BatchState.CANCELLED)
    with pytest.raises(IllegalTransition):
        assert_batch_transition(BatchState.RUNNING, BatchState.CANCELLED)


def test_recovery_target_rewinds_to_queued():
    assert recovery_target(FileState.DOWNLOADED.value) == FileState.QUEUED
    assert recovery_target(None) == FileState.QUEUED
    # Verified/done are not rewound.
    assert recovery_target(FileState.VERIFIED.value) == FileState.VERIFIED
    assert recovery_target(FileState.DONE.value) == FileState.DONE
