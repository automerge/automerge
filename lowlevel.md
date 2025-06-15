By default, a `DocHandle` wraps a JSON document representing the data in the underlying document which is retrieved with `DocHandle.doc()`. In some cases, the view of the document that you want is _not_ just the JSON view but instead some subset of the document, or perhaps a more complex derived view.

Similarly, when you call `DocHandle.change` you pass a callback which gets passed a JSON document to make modifications to. As with the view type, you don't always want to work with a JSON document, but instead with some other dervied view or with the low level Automerge document directly.

To do both these things `Repo.find` offers an API which allows you to customise this behavior. It looks like this:

```typescript

// DocHandle now has two generic parameters, one for the view type - which is
// retrieved from `DocHandle.doc()`, and one for the write type, which is passed
// to the `DocHandle.change` callback.
class DocHandle<View, Write> {
    doc(): View
    change(callback: (doc: Write) => void): void
}

// To create a `DocHandle` with a custom view and write type, you pass this
// configuration object to the `Repo.find` method:
const handle = repo.find("automerge:alsdkfjasdfjasldkfj", { 
    // This creates the initial view of the document
    createView: (doc: ReadDoc): View {
        ..
    },
    // This is called whenever the document changes to update the view
    updateView: (doc: ReadDoc, view: View, patches: Patch[]): View {
        ..
    }
    write: (doc: WriteDoc, callback) => {
        .. 
    }
})

interface ReadDoc {
    get(obj: ObjectId, key: string | index): AutomergeValue | undefined;
    ..
}

interface WriteDoc {
    set(obj: ObjectId, key: string | index, value: AutomergeValue): void;
    ..
}
```
