# Copyright 2021 Canonical Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import logging
import semantic_version as semver

from ops.charm import CharmEvents
from ops.framework import (
    Object, EventSource, EventBase, StoredState
)

logger = logging.getLogger(__name__)


class Provider(Object):
    """Manages relations of resource providers
    A :class:`Provider` object manages relations of a resource provider
    charms, with charms that consume those resources. When a consumer
    charm joins the relation a provider object informs it of the type
    and version of the resource provided. Providers may also be
    instrumented to inform consumer charms of any relavent
    information, for example configuration settings. This is done
    using the `provides` argument of the :class:`Provider` constructor.
    Any charm that provides a resource may choose to do so through the
    :class:`Provider` object simpliy by instantiating it in the charm's
    `__init__` method, as follows
    Example::
        self.provider = Provider(self, relation_name, provides)
        ...
        self.provider.ready()
    It is important to invoke `ready()` on the :class:`Provider`
    object, in order to let the consumer charm know that the provider
    is serving requests. This is done by setting a boolean flag
    `ready` in the data forwarded to the consumer charm. A provider
    charm may toggle this flag by invoking `unready()` when it
    is unable to service any requests for example during an
    upgrade. After an upgrade the :class:`Provider` object notifies
    consumer charms by resending type, version and any other data to
    the consumer. Even though :class:`Provider` objects only handle
    relation joined and provider upgrade events, they may be
    subclassed to extend their functionality in any way desired.
    Args:
        charm: :class:`ops.charm.CharmBase` derived object that is
            instantiating :class:`Provider`. This is almost always
            `self`.
        name: string name of relation (as defined in `metadata.yaml`) that
            consumer charms will use to interact with provider charms.
        provides: A dictionary containing the information that
            :class:`Provider` will forward to any consumer charm when it forms
            a relation with the provider charm. This dictionary must have at
            least one key `provides`. The value of this key itself must be a
            dictionary, whose key as a resource name and value is a version
            string. For example here is a suitable provides argument
            Example::
                {'provides': {'mongodb': '2.4'}}
    The `provides` argument dictionary may contain any other key value
    pairs. If so this information is forwarded to the consumer charm
    as application data.
    """
    stored = StoredState()

    def __init__(self, charm, name, provides):
        super().__init__(charm, name)

        self.stored.set_default(ready=False)
        self.name = name
        self.provides = provides

        events = charm.on[name]
        self.framework.observe(events.relation_joined, self.on_consumer_joined)
        self.framework.observe(charm.on.upgrade_charm, self.on_upgrade)

    def on_consumer_joined(self, event):
        """Handle consumer joined event
        Args:
            event: event object
        """
        data = self.provider_data()

        if self.model.unit.is_leader():
            logger.debug("Providing for joined consumer : {}".format(data))
            event.relation.data[self.model.app]['provider_data'] = json.dumps(data)

    def on_upgrade(self, event):
        """Handle a provider upgrade event
        Args:
            event: event object
        """
        self.notify_consumers()

    def notify_consumers(self):
        """Resend provider data to consumers
        """
        data = self.provider_data()
        if self.model.unit.is_leader():
            logger.debug("Notifying Consumer : {}".format(data))
            for rel in self.framework.model.relations[self.name]:
                rel.data[self.model.app]['provider_data'] = json.dumps(data)

    def ready(self):
        """Set provider state to ready
        """
        if not self.is_ready:
            logger.debug("Provider is ready")
            self.stored.ready = True
            self.notify_consumers()

    def unready(self):
        """Set provider state to unready
        """
        logger.debug("Provider is not ready")
        self.stored.ready = False
        self.notify_consumers()

    def provider_data(self):
        """Construct relation data packet for consumer
        """
        data = self.provides.copy()
        data['ready'] = self.stored.ready
        return data

    @property
    def is_ready(self):
        """Query state of provider
        """
        return self.stored.ready


class ProviderAvailable(EventBase):
    """Event triggered when a valid provider is found
    When a consumer charm forms a relation with a provider charm,
    their :class:`Consumer` and :class:`Provider` object exchange
    information to asertain compatibility. If they relation is found
    to be compatible then the :class:`Consumer` object raises a
    :class:`ProviderAvailable` event to inform the consumer charm, that
    a relation with the provider charm has been successful.
    """
    def __init__(self, handle, data=None):
        super().__init__(handle)
        self.data = data

    def snapshot(self):
        return {"data": self.data}

    def restore(self, snapshot):
        self.data = snapshot["data"]


class ProviderInvalid(EventBase):
    """Event triggered when a provider is not compatible
    When a consumer charm forms a relation with a provider charm,
    their :class:`Consumer` and :class:`Provider` object exchange
    information to asertain compatibility. If they relation is found
    not to be compatible then the :class:`Consumer` object raises a
    :class:`ProviderInvalid` event to inform the consumer charm, that
    a relation with the provider charm has *not* been successful.
    """
    def __init__(self, handle, data=None):
        super().__init__(handle)
        self.data = data

    def snapshot(self):
        return {"data": self.data}

    def restore(self, snapshot):
        self.data = snapshot["data"]


class ProviderUnready(EventBase):
    """Event triggered when a provider is not ready
    If a provider charm is not ready to service requests, when a
    consumer charm for a new relation with it, or is already related
    to it, then a :class:`ProviderUnready` event is raised. This
    presumes that the provider charm as set its `ready` status to
    `False`.
    The :class:`ProviderUnready` event is raised regardless of wether
    the provider charm is compatible or not. Compatibility checks are
    done only if the provider charm is ready to service requests.
    """
    pass


class ProviderBroken(EventBase):
    """Event raised when provider consumer relation is disolved
    If the realtion between a provider and consumer charm is removed,
    then a :class:`ProviderBroken` relation is raised.
    """
    pass


class ConsumerEvents(CharmEvents):
    """Descriptor for consumer charm events
    """
    available = EventSource(ProviderAvailable)
    invalid = EventSource(ProviderInvalid)
    unready = EventSource(ProviderUnready)
    broken = EventSource(ProviderBroken)


class Consumer(Object):
    """Manages relations with a resource provider
    The :class:`Consumer` object manages relations with resource
    provider charms, by checking compatibility between consumer
    requirements and provider type and version specification. Any
    charm that uses resources provided by other charms may manage its
    relation with the providers by instantiating on :class:`Consumer`
    object for each such relation. A :class:`Consumer` object may be
    instantiated in the `__init__` method of the consumer charm as
    follows
    Example::
        self.provider_name = Consumer(self, resource_name, consumes)
    In managing the relation between provider and consumer, the
    :class:`Consumer` object may raise any of the following events,
    that a consumer charm can choose to respond to
    - :class:`ProviderAvailable`
    - :class:`ProviderInvalid`
    - :class:`ProviderUnready`
    - :class:`ProviderBroken`
    Args:
        charm: :class:`ops.charm.CharmBase` derived object that is
            instantiating the :class:`Consumer` object. This is almost
            always `self`.
        name: string name of relation (as defined in `metadata.yaml`) that
            consumer charms will use to interact with provider charms.
        consumes: a dictionary containing acceptable provider
            specifications. The dictonary must contain at least one
            key `consumes`. The value of this key is itself a
            dictionary. This nested dictionary contains key value
            pairs that are all the acceptable provider
            specifications. The keys in these specifications are the
            resource names strings. And the values are version
            specification strings. The version specification strings
            can by in any form that is compatible with the
            [semver](https://pypi.org/project/semver/) Python
            package. A valid example of the `consumes` dictionary is
            Example::
            {'consumes': {'mysql': '>5.0.2', 'mariadb': '<=6.1.0'}}
        multi: a boolean flag that indicates if the :class:`Consumer` object
            should allow multiple relations with the same relation name. By
            default this is `False`.
    """
    on = ConsumerEvents()
    stored = StoredState()

    def __init__(self, charm, name, consumes, multi=False):
        super().__init__(charm, name)

        self.name = name
        self.consumes = consumes
        self.multi_mode = multi
        self.stored.set_default(relation_id=None)

        events = charm.on[name]
        self.framework.observe(events.relation_changed, self.on_provider_changed)
        self.framework.observe(events.relation_broken, self.on_provider_broken)
        self.framework.observe(charm.on.upgrade_charm, self.validate_provider)

    def on_provider_changed(self, event):
        """Validate provider on relation changed event
        This method checks the provider for compatibility with the
        consumer every time a relation changed event is raised. The
        provider is also checked to ensure it is ready to service
        requests. In response to these checks any of the following
        events may be raised.
        - :class:`ProviderAvailable`
        - :class:`ProviderInvalid`
        - :class:`ProviderUnready`
        Args:
            event: an event object. It is expected that the event object
                contains a key `provider_data` whose value is all the data
                forwarded by the :class:`Provider` object.
        """
        rdata = event.relation.data[event.app]
        logger.debug("Got data from provider : {}".format(rdata))
        provider_data = rdata.get('provider_data')
        consumed = self.consumes
        if provider_data:
            data = json.loads(provider_data)
            try:
                provides = data['provides']
            except KeyError:
                # provider has not set any specification
                # so no compatibility checks are done
                # and no events are raised
                logger.warning('Provider not specified')
                return
        else:
            logger.debug('No provider data')
            # provider has not given any information
            # so provider will not be made available (as yet)
            return

        ready = data.get('ready')
        if not ready:
            self.on.unready.emit()
            return

        stored_id = self.stored.relation_id
        rel_id = event.relation.id
        check_single = ((stored_id is None) or (stored_id == rel_id))
        if self.multi_mode or check_single:
            requirements_met = self.meets_requirements(provides, consumed)
        else:
            return

        if requirements_met:
            logger.debug('Got compatible provider : {}'.format(provider_data))
            if not self.multi_mode and not self.stored.relation_id:
                self.stored.relation_id = rel_id
                logger.debug('Saved relation id : {}'.format(rel_id))
            self.on.available.emit(data)
        else:
            logger.error('Incompatible provider : Need {}, Got {}'.format(
                consumed, provider_data))
            self.on.invalid.emit(provides)

    def on_provider_broken(self, event):
        """Inform consumer charm that provider relation no longer exists
        This method raises a :class:`ProviderBroken` event in respose to
        a relation broken event.
        Args:
            event: an event object
        """
        logger.debug("Provider Broken : {}".format(event))
        if not self.multi_mode:
            self.stored.relation_id = None
        self.on.broken.emit()

    def validate_provider(self, event):
        """Check provider and consumer compatibility
        This method validates provider consumer compatibility using
        data that is already available in the application relation
        bucket.
        Args:
            event: an event object
        """
        logger.debug("Validating provider(s) : {}".format(event))
        consumed = self.consumes

        for relation in self.framework.model.relations[self.name]:
            data = self.provider_data(relation.id)
            if data:
                try:
                    provides = data['provides']
                except KeyError:
                    continue

            requirements_met = self.meets_requirements(provides, consumed)
            if requirements_met:
                self.on.available.emit(data)
            else:
                logger.error('Provider non longer compatible, Need {}, have {}'.format(
                    consumed, data))
                self.on.invalid.emit(data)

    def meets_requirements(self, provides, consumes):
        """Check if provider and consumer are compatible.
        Args:
            provides: a dictionary with a single key value pair. The key
                is a string naming the resource provided. The value is a
                string given the version of the provided resource.
            consumes: a dictionary with zero or more key value paris. Each
                key is a string name of a resource that is acceptable. The
                corresponding value is a string representing an acceptable
                version specification. The version specification can be in any
                format that is compatible with the
                [semver](https://pypi.org/project/semver/) Python package.
        Returns:
            bool: True if the producer and consumer specification are
            compatible.
        """
        assert(len(provides) == 1)
        provided = tuple(provides.items())[0]
        for required in consumes.items():
            if self.is_compatible(provided, required):
                return True
        return False

    def is_compatible(self, has, needs):
        """Is a producer and consumer specification compatible.
        Args:
            has: a dictionary with a single key value pair. The key
                is a string naming the resource provided. The value is a
                string given the version of the provided resource.
            needs: a dictionary with a single key value pair. The key is a
                string naming an acceptable resources type. The value is a
                string specifying acceptable versions for the resource
                type. The version specification can be in any format that is
                compatible with the
                [semver](https://pypi.org/project/semver/) Python package.
        Returns:
            bool: True if the producer and consumer specification are
               compatible.
        """

        # if consumer has no constraints
        # compatibility is true by default
        if not needs:
            return True

        # if consumer has constraints but provider
        # has no specification compatibility can not
        # be determined and is hence false by default
        if not has and needs:
            return False

        # By now we know both consumer and provider have a
        # constraint specification so which check if the
        # constraint type is the same
        has_type = self.normalized_type(has)
        needs_type = self.normalized_type(needs)
        if has_type != needs_type:
            return False

        # By now we know consumer and provider have the
        # same constraint type so we check if the constraints
        # are futher qualified by version specifications

        # If consumer is not qualified, producer and
        # consumer are compatible by default
        if not self.has_version(needs):
            return True

        # If consumer is qualified but producer is not there
        # is no way to determine compatibility so it is False
        # by default
        if not self.has_version(has):
            return False

        # Both consumer and provider are qualified so we
        # check compatibiliity of version
        spec = semver.SimpleSpec(self.normalized_version(needs))
        got = semver.Version.coerce(self.normalized_version(has))

        return spec.match(got)

    def has_version(self, constraint):
        """Does the constraint have a version qualification
        Args:
            constraint: a tuple containing a resource type (first member)
                and optionally a resource version (second member)
        Returns:
            bool: True if a resource version is present in constraint.
        """
        if len(constraint) == 2:
            return True
        return False

    def normalized_version(self, spec):
        """Remove spaces from version strings.
        Args:
            spec: a tuple containing two members. The second member being
                a `semver` version specification.
        Returns:
            str: a version specification that has spaces removed in
                order to make it compatible with the `semver` package
                utilities.
        """
        version = spec[1]
        if ' ' in version:
            return "".join(version.split())
        else:
            return version

    def normalized_type(self, spec):
        """Extract and lowercase type from specification.
        Args:
            spec: a string naming a resource type
        Returns:
            str: all lowercase equivalent of spec string, in order to
                facilitate case insensitive comparison of resource types.
        """
        return spec[0].lower()

    def provider_data(self, rel_id=None):
        """Get provider relation data
        Args:
            rel_id: integer identity of relation for which data is
                required. If the :class:`Consumer` object was instantiated using
                `multi=True` then `rel_id` is a required argument, otherwise
                it is optional (and not used)
        Returns:
            dict: containing provider application relation relation data.
        """
        if self.multi_mode:
            assert(rel_id is not None)
            rel = self.framework.model.get_relation(self.name, rel_id)
        else:
            assert(len(self.framework.model.relations[self.name]) == 1)
            rel = self.framework.model.get_relation(self.name)

        data = json.loads(rel.data[rel.app]['provider_data'])
        return data
